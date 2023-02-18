import subprocess


def clean_up_logs(result):
    logs = []
    for i, l in enumerate(result.stderr.split("\n")):
        if len(l) == 0:
            continue
        if "- INFO -" in l:
            logs.append(l.split("- INFO -")[-1])
    if logs == []:
        logs.append("No INFO logs")
    return "|".join(logs)


bashCommand = 'gcloud dataproc batches list --region="europe-west3"'
result = subprocess.run([bashCommand], capture_output=True, text=True, shell=True)
for i, l in enumerate(result.stdout.split("\n")):
    if len(l) == 0:
        continue
    if "BATCH_ID" in l:
        continue
    batch_id = l.split(" ")[0]
    status = l.split(" ")[-1]
    # print(f"Removing {batch_id}")
    # if status in ["SUCCEEDED", "FAILED"]:
    if status in ["FAILED"]:
        try:
            bashCommand = f'gcloud dataproc batches wait {batch_id} --region="europe-west3" --project "dataops-369610"'
            result = subprocess.run(
                [bashCommand], capture_output=True, text=True, shell=True
            )
            log_text = clean_up_logs(result)
            print(f"{batch_id}: {log_text}")
        except Exception as e:
            print(e)
            break
