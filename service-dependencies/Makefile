
all: namespace.yaml quickinstall.yaml

quickinstall.yaml:
	curl --fail -o quickinstall.yaml https://strimzi.io/install/latest?namespace=lab-kafka

namespace.yaml:
	kubectl create ns lab-kafka --dry-run=client -o yaml > namespace.yaml
