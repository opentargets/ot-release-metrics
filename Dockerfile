# Part 1: Common set up. This can be eventually separated, for example as "ot-base" image.

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
ENV PYTHONPATH=/home/spark/packages
RUN mkdir -p "${PYTHONPATH}"




# Part 2: ot-release-metrics specific setup.

# Install requirements.
COPY requirements.txt /home/spark/requirements.txt
RUN python3 -m pip install -q -r /home/spark/requirements.txt --target "${PYTHONPATH}"

# Copy the package.
COPY src $PYTHONPATH/src
COPY config/config.yaml $PYTHONPATH/src/metric_calculation/config.yaml

# Define the entry point.
RUN chmod -R 777 /home/spark
WORKDIR /home/spark/packages/src/metric_calculation
ENTRYPOINT ["python3", "/home/spark/packages/src/metric_calculation/metrics.py"]
USER spark
