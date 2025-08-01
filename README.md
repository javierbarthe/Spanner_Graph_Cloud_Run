To Build:
gcloud builds submit --tag gcr.io/my-first-project-424319/shortest-path .
To Run:
gcloud run deploy shortest-path \
--image [image name] \
--platform managed \
--region us-central1 \
--set-env-vars SPANNER_INSTANCE_ID=xxx,SPANNER_DATABASE_ID=xxx \
--allow-unauthenticated
