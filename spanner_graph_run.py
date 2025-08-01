import os
import json
from flask import Flask, request, jsonify
from google.cloud import spanner
import google.cloud.logging

# Initialize Flask app
app = Flask(__name__)

# Initialize Google Cloud Logging
logging_client = google.cloud.logging.Client()
log_name = "spanner-graph-run"
logger = logging_client.logger(log_name)

# Initialize Spanner client
instance_id = os.environ.get("SPANNER_INSTANCE_ID", "jblab")
database_id = os.environ.get("SPANNER_DATABASE_ID", "jblab")

spanner_client = spanner.Client()
instance = spanner_client.instance(instance_id)
database = instance.database(database_id)

@app.route("/", methods=["GET"])
def run_graph_query():
    """
    Executes a Spanner GQL query and logs the output.
    Takes 'start_node' and 'end_node' as query parameters.
    """
    start_node = request.args.get("start_node")
    end_node = request.args.get("end_node")

    if not start_node or not end_node:
        error_message = "Missing 'start_node' or 'end_node' parameter."
        logger.log_text(error_message, severity="ERROR")
        return jsonify({"error": error_message}), 400

    try:
        start_node_id = int(start_node)
        end_node_id = int(end_node)
    except ValueError:
        error_message = "Invalid 'start_node' or 'end_node' parameter. Must be an integer."
        logger.log_text(error_message, severity="ERROR")
        return jsonify({"error": error_message}), 400

    gql_query = """
        Graph INVENTARIO2
        MATCH
        p = ANY SHORTEST (
        (src:NODO {{NODEID: {start_node}}})-[s:SEGMENTO | REVERSE_SEGMENTO]->{{1, 20}}(dest:NODO {{NODEID: {end_node}}})
        )
        LET es = EDGES(p)
        FOR element in es
        RETURN element.STARTNODEID, element.SEGMENTID, element.ENDNODEID
    """.format(start_node=start_node_id, end_node=end_node_id)

    outputs = []
    try:
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(gql_query)
            for row in results:
                output = {
                    "STARTNODEID": row[0],
                    "SEGMENTID": row[1],
                    "ENDNODEID": row[2],
                }
                outputs.append(output)

        # Log the entire result set as a structured log
        logger.log_struct(
            {
                "message": f"GQL query executed successfully for start_node={start_node_id} and end_node={end_node_id}",
                "query": gql_query,
                "result_count": len(outputs),
                "results": outputs,
            },
            severity="INFO",
        )

        return jsonify(outputs)

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        logger.log_text(error_message, severity="ERROR")
        return jsonify({"error": error_message}), 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
