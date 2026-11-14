
FROM python:3.12-slim

WORKDIR /workspace

RUN pip install --no-cache-dir \
    jupyterlab==4.4.10 \
    ipykernel==6.30.1 \
    pandas==2.3.3 \
    numpy==2.3.4 \
    sqlalchemy==2.0.44 \
    psycopg2-binary==2.9.11 \
    matplotlib==3.10.7 \
    seaborn==0.13.2 \
    python-dotenv==1.2.1 \
    requests==2.32.5 \
    geopandas==1.1.1 \
    shapely==2.1.2 \
    pyarrow==22.0.0

EXPOSE 8888

CMD ["jupyter", "lab", \
     "--ip=0.0.0.0", \
     "--port=8888", \
     "--no-browser", \
     "--allow-root", \
     "--ServerApp.token=sismic-local"]
