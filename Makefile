.PHONY: build push

DEPLOY_NAMESPACE = moop-dev
TAG			= $(shell git describe --tags --always)
REGISTRY	= registry.datadynamic.io/moop
IMAGE		= moop-cull-service


build: 
	docker build --rm -t "$(REGISTRY)/$(IMAGE):$(TAG)" -f Dockerfile .

push: build
	docker push "$(REGISTRY)/$(IMAGE):$(TAG)"

deploy: push
	# replace image tag on deployment.yaml
	sed -i 's/{IMAGE_TAG_for_change}/$(TAG)/g' deploy/cull-service-deployment.yaml
	# apply change
	kubectl apply -f deploy/ --namespace "$(DEPLOY_NAMESPACE)"
	# restore deployment.yaml
	sed -i 's/$(TAG)/{IMAGE_TAG_for_change}/g' deploy/cull-service-deployment.yaml
