FROM gcr.io/distroless/static
ENTRYPOINT ["/gcs-unzip"]
COPY gcs-unzip /
