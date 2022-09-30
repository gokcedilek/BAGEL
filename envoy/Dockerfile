FROM envoyproxy/envoy:v1.21.1
COPY envoy.yaml /etc/envoy/envoy.yaml
RUN chmod go+r /etc/envoy/envoy.yaml


# FROM envoyproxy/envoy:v1.12.2

# COPY ./envoy.yaml /etc/envoy/envoy.yaml

# CMD /usr/local/bin/envoy -c /etc/envoy/envoy.yaml