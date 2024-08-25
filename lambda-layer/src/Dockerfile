ARG runtime=python3.12

FROM public.ecr.aws/sam/build-${runtime}

ADD . /workspace

WORKDIR /workspace

RUN mkdir -p /build && \
  python3 -m pip install aws-opentelemetry-distro/ -t /build/python && \
  mv otel_wrapper.py /build/python && \
  mv otel-instrument /build && \
  chmod 755 /build/otel-instrument && \
  rm -rf /build/python/boto* && \
  rm -rf /build/python/urllib3* && \
  cd /build && \
  zip -r aws-opentelemetry-python-layer.zip otel-instrument python

CMD cp /build/aws-opentelemetry-python-layer.zip /out/aws-opentelemetry-python-layer.zip
