# hash:sha256:18e7f22167f55a3f50cd731a781ddb30cfe8b56a8aec20a8a11c5e7d06067233
ARG REGISTRY_HOST
FROM $REGISTRY_HOST/codeocean/jupyterlab:3.6.1-miniconda4.12.0-python3.9-ubuntu20.04

ARG DEBIAN_FRONTEND=noninteractive
ARG AWS_ACCESS_KEY_ID
ARG AWS_DEFAULT_REGION
ARG AWS_SECRET_ACCESS_KEY
ARG CUSTOM_KEY

ARG GIT_ASKPASS
ARG GIT_ACCESS_TOKEN
COPY git-askpass /

RUN pip install -U --no-cache-dir \
	s3fs==2024.10.0 \
	aind-codeocean-api==0.4.2 \
	aind-data-schema==1.1.1 \
