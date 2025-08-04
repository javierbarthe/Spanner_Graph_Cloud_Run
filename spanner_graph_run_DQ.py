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

    try:
        # First query to get the path length
        path_length_query = """
            Graph INVENTARIO2
            MATCH
            ANY SHORTEST (
            (src:NODO {{NODEID: {start_node}}})-[s:SEGMENTO | REVERSE_SEGMENTO]->{{1, 100}}(dest:NODO {{NODEID: {end_node}}})
            )
            LET path_length = COUNT(s)
            RETURN  path_length
        """.format(start_node=start_node_id, end_node=end_node_id)

        path_length = 0
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(path_length_query)
            for row in results:
                path_length = row[0]
                break  # We only expect one row

        if path_length == 0:
            error_message = "No path found between the specified nodes."
            logger.log_text(error_message, severity="WARNING")
            return jsonify({"message": error_message, "path_length": 0}), 200

        outputs = []
        gql_query = ""
        if path_length > 20:
            remaining_path_length = path_length
            current_start_node = start_node_id
            
            while remaining_path_length > 0:
                chunk_size = min(20, remaining_path_length)
                
                intermediate_end_node = None
                # For the last chunk, the destination is the original end_node
                if remaining_path_length <= 20:
                    intermediate_end_node = end_node_id
                else:
                    end_node_query = """
                        Graph INVENTARIO2
                        MATCH
                        ANY SHORTEST (
                        (src:NODO {{NODEID: {start_node}}})-[s:SEGMENTO | REVERSE_SEGMENTO]->{{1, {chunk_size}}}(dest:NODO))
                        LET path_length = COUNT(s)
                        FILTER path_length = {chunk_size}
                        RETURN path_length,dest.NODEID as end_node
                    """.format(start_node=current_start_node, chunk_size=chunk_size)

                    with database.snapshot() as snapshot:
                        results = snapshot.execute_sql(end_node_query)
                        for row in results:
                            intermediate_end_node = row[1]
                            break

                if intermediate_end_node is None:
                    error_message = f"Could not find an intermediate node from {current_start_node} at distance {chunk_size}"
                    logger.log_text(error_message, severity="ERROR")
                    return jsonify({"error": error_message}), 500

                gql_query_chunk = """
                    Graph INVENTARIO2
                    MATCH
                    p = ANY SHORTEST (
                    (src:NODO {{NODEID: {start_node}}})-[s:SEGMENTO | REVERSE_SEGMENTO]->{{1, {chunk_size}}}(dest:NODO {{NODEID: {end_node}}})
                    )
                    LET es = EDGES(p)
                    FOR element in es
                    RETURN element.STARTNODEID, element.SEGMENTID, element.ENDNODEID
                """.format(start_node=current_start_node, end_node=intermediate_end_node, chunk_size=chunk_size)
                
                gql_query += gql_query_chunk

                with database.snapshot() as snapshot:
                    results = snapshot.execute_sql(gql_query_chunk)
                    for row in results:
                        output = {
                            "STARTNODEID": row[0],
                            "SEGMENTID": row[1],
                            "ENDNODEID": row[2],
                        }
                        outputs.append(output)
                
                remaining_path_length -= chunk_size
                current_start_node = intermediate_end_node
        else:
            # Main query using the dynamic path length
            gql_query = """
                Graph INVENTARIO2
                MATCH
                p = ANY SHORTEST (
                (src:NODO {{NODEID: {start_node}}})-[s:SEGMENTO | REVERSE_SEGMENTO]->{{1, {path_length}}}(dest:NODO {{NODEID: {end_node}}})
                )
                LET es = EDGES(p)
                FOR element in es
                RETURN element.STARTNODEID, element.SEGMENTID, element.ENDNODEID
            """.format(start_node=start_node_id, end_node=end_node_id, path_length=path_length)

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
