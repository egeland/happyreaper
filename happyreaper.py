from delorean import now, Delorean
from kubernetes import client, config
import boto3
import os


def container_info(pod):
    restart_count = 0
    container_creating = False
    for c in pod.status.container_statuses:
        restart_count += c.restart_count
        if c.state.waiting and c.state.waiting.reason == "ContainerCreating":
            container_creating = True
    return (restart_count, container_creating)


def is_statefulset(pod):
    for ref in pod.metadata.owner_references:
        if ref.kind == "StatefulSet":
            return True
    return False


def detach_volume(pod):
    pvc = v1.read_namespaced_persistent_volume_claim(name=find_pvc(pod), namespace=pod.metadata.namespace)
    volume_name = pvc.spec.volume_name
    pv = v1.read_persistent_volume(volume_name)
    volume_id = pv.spec.aws_elastic_block_store.volume_id
    if not ec2:
        ec2 = boto3.client(service_name="ec2", region_name=pv.metadata.labels['failure-domain.beta.kubernetes.io/region'])
    ec2.detach_volume(VolumeId=volume_id, Force=True)


def find_pvc(pod):
    for vol in pod.spec.volumes:
        if vol.persistent_volume_claim and vol.persistent_volume_claim.claim_name:
            return vol.persistent_volume_claim.claim_name
    return None


def delete_pod(pod):
    v1.delete_namespaced_pod(name=pod.metadata.name, namespace=pod.metadata.namespace)


def main():
    MAX_AGE = int(os.getenv("MAX_AGE", 60)) * 60  # minutes (convert to sec)
    MAX_RESTART = int(os.getenv("MAX_RESTART", 20))
    config.load_incluster_config()

    global ec2
    ec2 = None
    global v1
    v1 = client.CoreV1Api()
    ret = v1.list_pod_for_all_namespaces(watch=False)
    current_time = now()
    for pod in ret.items:
        if pod.status.phase in ["Running", "Succeeded"]:
            continue
        restart_count, is_container_creating = container_info(pod)
        if is_container_creating:
            # figure out age
            elapsed_time = current_time - Delorean(pod.status.start_time)
            elapsed_time = int(elapsed_time.total_seconds())
            if elapsed_time > MAX_AGE:
                # if it's been stuck for more than MAX_AGE
                # and it is a statefulset, we should detach its volume,
                # otherwise delete the pod
                if is_statefulset(pod):
                    detach_volume(pod)
                else:
                    delete_pod(pod)
                continue
        if restart_count >= MAX_RESTART:
            delete_pod(pod)


if __name__ == '__main__':
    main()
