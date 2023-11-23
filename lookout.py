import os
from copy import copy

from kubernetes import client, config, watch
import slack

def _generate_progress_bar(position, max_value):
    if position is None:
        position = 0

    filled_squares = (100 / max_value * position) / 5

    filled_char = "⬛"
    empty_char = "⬜"
    return (filled_char * int(filled_squares)) + (
            empty_char * (20 - int(filled_squares))) + "\n"


class KubeLookout:
    template = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": ""
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": ""
            },
            "accessory": {
                "type": "image",
                "image_url": "",
                "alt_text": "status image"
            }
        }
    ]

    def __init__(self, warning_image, progress_image, ok_image,
                 slack_key, slack_channel, 
                 cluster_name, gcp_region, gcp_project):
        super().__init__()
        self.warning_image = warning_image
        self.ok_image = ok_image
        self.progress_image = progress_image
        self.slack_client = None
        self.slack_key = slack_key
        self.slack_channel = slack_channel
        self.cluster_name = cluster_name
        self.gcp_region = gcp_region
        self.gcp_project = gcp_project
        self.deployment_thread = None
        self.deployment_count = 0
        self.rollouts = {}
        self.degraded = set()

    def _init_client(self):
        if "KUBERNETES_PORT" in os.environ:
            config.load_incluster_config()
        else:
            config.load_kube_config()
        api_client = client.api_client.ApiClient()
        self.core = client.AppsV1Api(api_client)

    def _send_slack_block(self, blocks, channel, message_id=None, thread_ts=None):
        if self.slack_client is None:
            self.slack_client = slack.WebClient(
                self.slack_key)
        if message_id is None:
            response = self.slack_client.chat_postMessage(channel=channel,
                                                          blocks=blocks,
                                                          icon_emoji=':kubernetes:',
                                                          thread_ts=thread_ts,
                                                          unfurl_links='false')
            return response.data['ts'], response.data['channel']
        response = self.slack_client.chat_update(
            channel=channel,
            thread_ts=thread_ts,
            ts=message_id, blocks=blocks)
        return response.data['ts'], response.data['channel']

    def _handle_deployment_change(self, deployment):
        metadata = deployment.metadata
        # we are not going to concern ourselves with changes to Kubernetes system stuff
        if (metadata.namespace == 'kube-system'):
            return
        deployment_key = f"{metadata.namespace}/{metadata.name}"
        print(f"Handling deployment of {deployment_key} in thread {self.deployment_thread[0]}")

        ready_replicas = 0
        if deployment.status.ready_replicas is not None:
            ready_replicas = deployment.status.ready_replicas

        if deployment_key not in self.rollouts and \
                deployment.status.updated_replicas is None:
            blocks = self._generate_deployment_rollout_block(deployment)
            resp = self._send_slack_block(blocks, self.slack_channel, thread_ts=self.deployment_thread[0])
            self.rollouts[deployment_key] = resp
            self.deployment_count += 1
            print(f"rollout added: {deployment_key}")

        elif deployment_key in self.rollouts:
            rollout_complete = (
                    deployment.status.updated_replicas ==
                    deployment.status.replicas ==
                    ready_replicas)
            blocks = self._generate_deployment_rollout_block(deployment,
                                                             rollout_complete)
            self.rollouts[deployment_key] = self._send_slack_block(
                channel=self.rollouts[deployment_key][1],
                message_id=self.rollouts[deployment_key][0], blocks=blocks,
                thread_ts=self.deployment_thread[0])

            if rollout_complete:
                self.rollouts.pop(deployment_key)
            print(f"rollout updated: {deployment_key} (complete: {rollout_complete})")

        elif ready_replicas < deployment.spec.replicas:
            print(f"Detected degraded {deployment.metadata.namespace}/{deployment.metadata.name}" +
                  f" {ready_replicas} ready out of {deployment.spec.replicas}")
            # blocks = self._generate_deployment_degraded_block(deployment)
            # self._send_slack_block(blocks, self.slack_channel)
            # self.degraded.add(deployment_key)

        elif (deployment_key in self.degraded and \
              ready_replicas >= deployment.spec.replicas):
            print(f"Recovered degraded {deployment.metadata.namespace}/{deployment.metadata.name}" +
                  f" {ready_replicas} ready out of {deployment.spec.replicas}")
            # self.degraded.remove(deployment_key)
            # blocks = self._generate_deployment_not_degraded_block(deployment)
            # self._send_slack_block(blocks, self.slack_channel)

    def _setup_deployment_thread(self):
        if self.deployment_thread is None:
            blocks = self._generate_deployment_thread_block()
            resp = self._send_slack_block(blocks, self.slack_channel)
            print(f"Started new thread {resp[0]} (rollouts: {self.rollouts})")
            self.deployment_thread = resp

    def _update_deployment_thread(self):
        print(f"Updating thread head {self.deployment_thread[0]} (rollouts: {len(self.rollouts)}, deploys: {self.deployment_count})")
        print(f"Sending update to {self.deployment_thread[1]}")
        try:
            if len(self.rollouts) == 0:
                blocks = self._generate_deployment_thread_block("complete")
                resp = self._send_slack_block(blocks=blocks, channel=self.deployment_thread[1], message_id=self.deployment_thread[0])
                self.deployment_thread = None
                self.deployment_count = 0
            else:
                blocks = self._generate_deployment_thread_block()
                resp = self._send_slack_block(blocks=blocks, channel=self.deployment_thread[1], message_id=self.deployment_thread[0])
        except Exception as e:
            print(f"Failed to update slack block: {e}")

    def _handle_event(self, deployment):
        if deployment.metadata.namespace != 'kube-system':
            self._setup_deployment_thread()
            self._handle_deployment_change(deployment)
            self._update_deployment_thread()

    def main_loop(self):
        while True:
            self._init_client()
            pods = self.core.list_deployment_for_all_namespaces(watch=False)
            resource_version = pods.metadata.resource_version
            stream = watch.Watch().stream(
                self.core.list_deployment_for_all_namespaces,
                resource_version=resource_version
            )
            print("Watching for deployment events")
            for event in stream:
                # print("Event: %s %s" % (event['type'], event['object'].metadata.name))
                deployment = event['object']
                self._handle_event(deployment)

    def _generate_deployment_rollout_block(self, deployment,
                                           rollout_complete=False):

        block = copy(self.template)
        header = f"*{self.gcp_project} deployment " \
            f"{deployment.metadata.namespace}/{deployment.metadata.name}" \
            f" is rolling out an update.*"
        message = ''
        for container in deployment.spec.template.spec.containers:
            message += f"Container {container.name} has image " \
                f"_ {container.image} _" \
                f"_ https://console.cloud.google.com/kubernetes/deployment/{self.gcp_region}/{self.cluster_name}/{deployment.metadata.namespace}/{deployment.metadata.name}/overview?project={self.gcp_project} _\n"
        message += "\n"

        # this math assumes a certain deployment logic -- spin up some new replicas before
        # spinning down the old -- but it's the logic we usually use
        unavailable = 0 if str(deployment.status.unavailable_replicas) == 'None' else deployment.status.unavailable_replicas
        updated = 0 if str(deployment.status.updated_replicas) == 'None' else deployment.status.updated_replicas
        print(deployment.metadata.namespace + "/" + deployment.metadata.name + \
              " unavailable: " + str(unavailable) + \
              " updated: " + str(updated))
        for condition in deployment.status.conditions:
            print("%s %s/%s %s=%s (%s)" % (condition.last_update_time,
                                           deployment.metadata.namespace,
                                           deployment.metadata.name,
                                           condition.type,
                                           condition.status,
                                           condition.message))
            
        live_updates = 0 if (updated < unavailable) else updated - unavailable
    
        message += f"{live_updates} replicas " \
            f"updated out of " \
            f"{deployment.spec.replicas}, {deployment.status.ready_replicas}" \
            f" ready.\n\n"
        message += _generate_progress_bar(
            live_updates, deployment.spec.replicas)

        block[0]['text']['text'] = header
        block[1]['text']['text'] = message
        block[1]['accessory']['image_url'] = self.progress_image
        # if the deployment status reflects that it is no longer progressing,
        # update the image to reflect that
        if deployment.status.conditions[-1].type == "Progressing" and \
            deployment.status.conditions[-1].status == "False":
            block[1]['accessory']['image_url'] = self.warning_image
            block[0]['text']['text'] = f"*{self.gcp_project} deployment " \
                f"{deployment.metadata.namespace}/{deployment.metadata.name}" \
                f" is failing: {deployment.status.conditions[-1].message}*"
        # when rollout is complete, update our image
        if rollout_complete:
            block[1]['accessory'][
                'image_url'] = self.ok_image
        return block

    def _generate_deployment_degraded_block(self, deployment):

        block = copy(self.template)

        header = f"*{self.gcp_project} deployment " \
            f"{deployment.metadata.namespace}/{deployment.metadata.name}" \
            f" has become degraded.*"

        ready_replicas = 0 if deployment.status.ready_replicas == 'None' else deployment.status.ready_replicas
        message = f"Deployment " \
            f"{deployment.metadata.namespace}/{deployment.metadata.name}" \
            f" has {ready_replicas} ready replicas " \
            f"when it should have {deployment.spec.replicas}.\n"

        message += _generate_progress_bar(deployment.status.ready_replicas,
                                          deployment.spec.replicas)

        block[0]['text']['text'] = header
        block[1]['text']['text'] = message
        block[1]['accessory'][
            'image_url'] = self.warning_image

        return block

    def _generate_deployment_not_degraded_block(self, deployment):
        block = copy(self.template)

        header = f"*{self.gcp_project} deployment " \
            f"{deployment.metadata.namespace}/{deployment.metadata.name}" \
            f" is no longer in a degraded state.*"

        message = f"Deployment " \
            f"{deployment.metadata.namespace}/{deployment.metadata.name}" \
            f" has {deployment.status.ready_replicas} ready " \
            f"replicas out of " \
            f"{deployment.spec.replicas}.\n"

        message += _generate_progress_bar(deployment.status.ready_replicas,
                                          deployment.spec.replicas)

        block[0]['text']['text'] = header
        block[1]['text']['text'] = message
        block[1]['accessory'][
            'image_url'] = self.ok_image

        return block

    def _generate_deployment_thread_block(self, status="in progress"):

        block = copy(self.template)
        if self.deployment_count == 0: bar_max = 1
        else: bar_max = self.deployment_count
        print(f"Thread head bar: deploys={self.deployment_count} rollouts={len(self.rollouts)} (so bar_max={bar_max})")
        header = f"*A kubernetes deployment in {self.gcp_project} is now {status}*"
        message = f"See the slack thread under this message for details\n"
        message += _generate_progress_bar(bar_max - len(self.rollouts), bar_max)

        block[0]['text']['text'] = header
        block[1]['text']['text'] = message
        if status == "complete":
            block[1]['accessory']['image_url'] = self.ok_image
        else:
            block[1]['accessory']['image_url'] = self.progress_image

        return block

if __name__ == "__main__":
    env_warning_image = os.environ.get("WARNING_IMAGE",
                                       "https://www.rocketlawyer.com/images/ops/warning.png")
    env_progress_image = os.environ.get("PROGRESS_IMAGE",
                                        "https://www.rocketlawyer.com/images/ops/progress.gif")
    env_ok_image = os.environ.get("OK_IMAGE",
                                  "https://www.rocketlawyer.com/images/ops/ok.png")
    env_slack_token = os.environ["SLACK_TOKEN"]
    env_slack_channel = os.environ.get("SLACK_CHANNEL", "#robot_dreams")
    env_cluster_name = os.environ.get("CLUSTER_NAME", "kubernetes")
    env_gcp_region = os.environ.get("GCP_REGION", "us-west1")
    env_gcp_project = os.environ.get("GCP_PROJECT", "rl-us")
    kube_deploy_watch = KubeLookout(env_warning_image,
                                    env_progress_image,
                                    env_ok_image, env_slack_token,
                                    env_slack_channel, env_cluster_name,
                                    env_gcp_region, env_gcp_project)

    kube_deploy_watch.main_loop()
