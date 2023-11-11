# Part 1: Common set up. This can be eventually separated into a shared base Open Targets image.

FROM python:3.10

# Suppress interactive prompts.
ENV DEBIAN_FRONTEND=noninteractive

# Install utilities required by Spark scripts.
RUN apt -qq update && apt -qq install -y procps tini libjemalloc2

# Enable jemalloc2 as default memory allocator.
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2

# Create the 'spark' group/user.
RUN groupadd -g 1099 spark
RUN useradd -u 1099 -g 1099 -d /home/spark -m spark

# Prepare for additional Python package installation.
ENV PIP_PACKAGES=/home/spark/packages
RUN mkdir -p "${PIP_PACKAGES}"




# Part 2: ot-release-metrics specific setup.

# Install main requirements.
COPY requirements.txt /home/spark/requirements.txt
RUN python3 -m pip install -q --target "${PIP_PACKAGES}" -r /home/spark/requirements.txt

# Install PIS and its requirements.
ENV PIS_PATH=/home/spark/platform-input-support
RUN python3 -m pip install -q --target "${PIP_PACKAGES}" configargparse strenum jsonpickle google-cloud-storage addict yapsy
RUN git clone -b master https://github.com/opentargets/platform-input-support $PIS_PATH

# Copy the main package.
COPY src ${PIP_PACKAGES}/src

# Define Dataproc Serverless parameters.
ENV PYTHONPATH="${PIP_PACKAGES}:${PIS_PATH}"
RUN chmod -R 777 /home/spark
USER spark
