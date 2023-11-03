IMAGE := gcr.io/rl-build-services-dev/kube-lookout

test:
	pytest

image:
	docker build -t $(IMAGE) .

push-image:
	docker tag $(IMAGE) && docker push $(IMAGE) || exit 0


.PHONY: image push-image test
