# kube-lookout
A utility to post changes to kubernetes deployments to Slack. Developed particularly for use with flux but can be used with any Kubernetes set-up. Tested with GKE 1.26.

## What does it do?

It listens to kubernetes deployment states and is interested when:

1. Any kubernetes deployment is rolling out a new version
2. If a kubernetes deployment is not healthy (Ready replicas is less than expected replicas)

It posts nice dynamic status updates for the above to slack

*Please don't use this to replace more mature monitoring tools (prometheus etc)*, this is for informational purposes only.

## For early adopters

Check deploy/* for example manifests.

The following ENV variables are used:

1. SLACK_TOKEN - A Slack Bot User OAuth Access Token - mandatory
2. SLACK_CHANNEL - Slack channel, defaults to "#robot_dreams"
3. CLUSTER_NAME - this is prefixed for most messages on slack, defaults to "Kubernetes Cluster"
4. PROGRESS_IMAGE, OK_IMAGE, WARNING_IMAGE - the URLs of images used for the slack notifications. Defaults to kinda ugly creative commons images, but does the job. (dont use SVGs I learned.)
5. THREAD_REFRESH - After the deployment thread is this many seconds old, post the update to the channel so the thread is easier to find
6. THREAD_TIMEOUT - After the deployment thread is this many seconds old, declare it failed.  New deploys after this will start a new thread.
7. GCP_REGION, GCP_PROJECT - used to generate links to the deployment in the GCP web console


## Screenshots

## Deployment Rollout
![Rollout Slack Notification - in progress](images/example3.png)
![Rollout Slack Notification](images/example2.png)
## Degraded deployment
![Degraded Slack Notification](images/example1.png)

## Uninstalling
kubectl delete serviceaccount/kube-lookout -n kube-system
kubectl delete clusterrole.rbac.authorization.k8s.io/kube-lookout -n kube-system
kubectl delete clusterrolebinding.rbac.authorization.k8s.io/kube-lookout -n kube-system
kubectl delete deployment.apps/kube-lookout -n kube-system
