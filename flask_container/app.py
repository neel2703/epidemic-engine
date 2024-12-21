from flask import Flask, jsonify, request, render_template, send_file
from dotenv import load_dotenv
import os
from datetime import timedelta, datetime
from sqlalchemy import Column, Integer, String, DateTime
from flask_sqlalchemy import SQLAlchemy
from io import BytesIO
from flask_cors import CORS
from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import StringIndexerModel, VectorAssembler
from plotly.graph_objs import Bar, Layout, Figure
from pyspark.sql.functions import unix_timestamp, month, col, when


app = Flask(__name__)
load_dotenv()
DATABASE_URI = os.getenv('DATABASE_URI')
CORS(app, supports_credentials=True, origins=["http://localhost:3000"])
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URI
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SESSION_COOKIE_SAMESITE'] = 'None'  # Allow cookies across domains
app.config['SESSION_COOKIE_SECURE'] = True

db = SQLAlchemy(app)

# SQLAlchemy Event model
class Event(db.Model):
    __tablename__ = 'events'
    id = Column(Integer, primary_key=True)
    Location = Column(String(150), nullable=False)
    Severity = Column(String(150), nullable=False)
    EventType = Column(String(150), nullable=False)
    Details = Column(String(150), nullable=False)
    Timestamp = Column(DateTime, nullable=False)

# Constants for form options
LOCATIONS = ["World", "London", "New York", "Paris", "Boston", "Los Angeles", "Berlin"]
SEVERITIES = ["high", "medium", "low"]
EVENTTYPES = ["hospital_admission", "general_health_report", "health_mention", "emergency_incident", "routine_checkup", "vaccination"]




spark = SparkSession.builder.appName("AnomalyDetectionApp").config("spark.driver.memory", "1g").config("spark.executor.memory", "512m").getOrCreate()
model_path = "./src/"
rf_model = RandomForestClassificationModel.load(model_path+"rf_model_health_risk")
event_type_indexer = StringIndexerModel.load(model_path+"EventType_indexer")
location_indexer = StringIndexerModel.load(model_path+"Location_indexer")


@app.route("/")
def home():
    return render_template("index.html", locations=LOCATIONS)

@app.route("/events", methods=["POST"])
def get_events():
    location = request.form.get("location")
    time_frame = request.form.get("time_frame")

    now = datetime.now()
    if time_frame == "week":
        start_time = now - timedelta(weeks=1)
    elif time_frame == "month":
        start_time = now - timedelta(days=30)
    elif time_frame == "year":
        start_time = now - timedelta(days=365)
    else:
        return jsonify({"error": "Invalid time frame"}), 400

    if location == "World":
        # Include all locations
        events = Event.query.filter(Event.Timestamp >= start_time).all()
    elif location in LOCATIONS:
        events = Event.query.filter(Event.Location == location, Event.Timestamp >= start_time).all()
    else:
        return jsonify({"error": "Invalid location"}), 400

    if not events:
        return jsonify({"error": "No events found for the given criteria"}), 404
    
    events_with_anomalies = detect_anomalies(events)
    data = aggregate_data(events_with_anomalies, time_frame)
    graph = generate_graph(data, time_frame)
    return graph

def detect_anomalies(events):
    # Convert SQLAlchemy events into Spark DataFrame
    spark_df = spark.createDataFrame([
        {
            "Severity": event.Severity,
            "EventType": event.EventType,
            "Location": event.Location,
            "Timestamp": event.Timestamp
        }
        for event in events
    ])

    # Feature engineering
    spark_df = spark_df.withColumn("Timestamp", unix_timestamp(col("Timestamp")).cast("timestamp"))
    spark_df = spark_df.withColumn("Month", month(col("Timestamp")))

    spark_df = event_type_indexer.transform(spark_df)
    spark_df = location_indexer.transform(spark_df)
    spark_df = spark_df.withColumn(
        "SeverityNumeric",
        when(col("Severity") == "low", 0)
        .when(col("Severity") == "medium", 1)
        .when(col("Severity") == "high", 2)
        .otherwise(None)
    )

    # Assemble features
    feature_cols = ["EventTypeIndex", "LocationIndex", "SeverityNumeric", "Month"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    spark_df = assembler.transform(spark_df)

    # Predict anomalies
    predictions = rf_model.transform(spark_df)

    # Add anomaly predictions to events
    predictions = predictions.select("prediction").collect()
    for i, event in enumerate(events):
        event.Anomaly = predictions[i]["prediction"] == 1

    return events


def aggregate_data(events, time_frame):
    data = {"severity": {}, "event_type": {}, "anomalies": 0, "total_events": 0}

    for severity in SEVERITIES:
        data["severity"][severity] = 0
    for event_type in EVENTTYPES:
        data["event_type"][event_type] = 0 

    for event in events:
        data["severity"][event.Severity] += 1
        data["event_type"][event.EventType] += 1
        data["total_events"] += 1
        if event.Anomaly:
            data["anomalies"] += 1

    if time_frame == "week":
        bucket_key = lambda event: event.Timestamp.date()
    elif time_frame == "month":
       bucket_key = lambda event: (event.Timestamp - timedelta(days=event.Timestamp.weekday())).date()
    elif time_frame == "year":
        bucket_key = lambda event: event.Timestamp.month
    elif time_frame == "decade":
        bucket_key = lambda event: event.Timestamp.year

    buckets = {}
    for event in events:
        key = bucket_key(event)
        if key not in buckets:
            buckets[key] = {"severity": {}, "event_type": {}, "anomalies": 0, "total_events": 0}
            for severity in SEVERITIES:
                buckets[key]["severity"][severity] = 0
            for event_type in EVENTTYPES:
                buckets[key]["event_type"][event_type] = 0

        buckets[key]["severity"][event.Severity] += 1
        buckets[key]["event_type"][event.EventType] += 1
        buckets[key]["total_events"] += 1
        if event.Anomaly:
            buckets[key]["anomalies"] += 1

    return buckets


def generate_graph(data, time_frame):
    from plotly.graph_objs import Bar, Layout, Figure
    from flask import send_file
    from io import BytesIO

    # Prepare x-axis labels based on time_frame
    if time_frame == "week":
        x_labels = [key.strftime('%Y-%m-%d') for key in data.keys()]
    elif time_frame == "month":
        x_labels = [f"{key} - {key + timedelta(days=6)}" for key in sorted(data.keys())]
    elif time_frame == "year":
        x_labels = [f"Month {key}" for key in data.keys()]
    elif time_frame == "decade":
        x_labels = [f"Year {key}" for key in data.keys()]
    else:
        x_labels = []

    # Extract severity, event type, anomaly, and total data
    severity_data = {severity: [bucket["severity"].get(severity, 0) for bucket in data.values()] for severity in SEVERITIES}
    event_type_data = {event_type: [bucket["event_type"].get(event_type, 0) for bucket in data.values()] for event_type in EVENTTYPES}
    anomaly_data = [bucket["anomalies"] for bucket in data.values()]
    total_events_data = [bucket["total_events"] for bucket in data.values()]

    # Prepare traces for severity
    severity_traces = [
        Bar(
            x=x_labels,
            y=counts,
            name=f"Severity: {severity.capitalize()}",
            visible=False,  # Initially hidden
            marker=dict(color=color)
        )
        for severity, counts, color in zip(SEVERITIES, severity_data.values(), ["red", "orange", "green"])
    ]

    # Prepare traces for event types
    event_type_traces = [
        Bar(
            x=x_labels,
            y=counts,
            name=f"Event Type: {event_type.capitalize()}",
            visible=False,  # Initially hidden
        )
        for event_type, counts in event_type_data.items()
    ]

    # Prepare trace for anomalies
    anomaly_trace = Bar(
        x=x_labels,
        y=anomaly_data,
        name="Anomalies",
        visible=False,  # Initially hidden
        marker=dict(color="purple")
    )

    # Prepare trace for total events (default visible)
    total_events_trace = Bar(
        x=x_labels,
        y=total_events_data,
        name="Total Events",
        visible=True,  # Default visible
        marker=dict(color="blue")
    )

    # Combine all traces
    all_traces = [total_events_trace] + severity_traces + event_type_traces + [anomaly_trace]

    # Create layout with checkboxes
    layout = Layout(
        title="Total Events",
        xaxis=dict(title="Dates"),
        yaxis=dict(title="Counts"),
        barmode="stack",  # Stacked bars
        updatemenus=[
            dict(
                type="buttons",  # Use buttons for checkboxes
                direction="right",
                showactive=True,
                x=0.5,
                y=1.15,
                buttons=[
                    dict(
                        label="Total Events",
                        method="update",
                        args=[
                            {"visible": [True] + [False] * (len(all_traces) - 1)},
                            {"title": "Total Events"}
                        ],
                    ),
                    dict(
                        label="Severities",
                        method="update",
                        args=[
                            {"visible": [False] + [True] * len(SEVERITIES) + [False] * (len(EVENTTYPES) + 1)},
                            {"title": "Severities"}
                        ],
                    ),
                    dict(
                        label="Event Types",
                        method="update",
                        args=[
                            {"visible": [False] * (1 + len(SEVERITIES)) + [True] * len(EVENTTYPES) + [False]},
                            {"title": "Event Types"}
                        ],
                    ),
                    dict(
                        label="Anomalies",
                        method="update",
                        args=[
                            {"visible": [False] * (1 + len(SEVERITIES) + len(EVENTTYPES)) + [True]},
                            {"title": "Anomalies"}
                        ],
                    ),
                ],
            )
        ],
    )

    # Create figure
    fig = Figure(data=all_traces, layout=layout)

    # Save graph to buffer and send it as a response
    buffer = BytesIO()
    html_str = fig.to_html(full_html=False, include_plotlyjs="cdn")  # Generate HTML
    buffer.write(html_str.encode("utf-8"))  # Write the HTML string as bytes
    buffer.seek(0)
    return send_file(buffer, mimetype="text/html")










# Run Flask app
if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=5001)
