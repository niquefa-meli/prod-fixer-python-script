import pandas, requests, json, multiprocessing, subprocess, time

MOD = 100
BLOCK = 10
TOKEN = None


def refresh_token():
    result = subprocess.run(["fury", "get-token"], stdout=subprocess.PIPE)
    TOKEN = result.stdout.decode("utf-8").replace("\n", "")
    print(f"TOKEN REFRESHED {TOKEN}")


def refresh_token_cron():
    while True:
        time.sleep(60 * 60)
        refresh_token()


def process(shipment_id: str, pack_id: str):
    res = requests.get(
        f"https://internal-api.mercadolibre.com/shipments/{shipment_id}/checkpoints?caller.scopes=admin",
        headers={"x-auth-token": TOKEN},
    )
    res = json.loads(res.text)
    checkpoint7000 = list(filter(lambda d: d["code"] == "00-7000", res["checkpoints"]))[0]
    checkpoint1001 = list(filter(lambda d: d["code"] == "00-1001", res["checkpoints"]))[0]

    res = requests.get(
        f"https://internal-api.mercadolibre.com/packs/{pack_id}?caller.scopes=admin&client.id=123",
        headers={"x-auth-token": TOKEN},
    )
    res = json.loads(res.text)
    return checkpoint7000["checkpoint_date"], checkpoint1001["checkpoint_date"], res["date_created"]


def persist_results(results):
    pandas.DataFrame(results).to_csv(
        "output.csv",
        mode="a",
        index=False,
        header=False,
        columns=["shipment_id", "pack_id", "checkpoint_date_7000", "checkpoint_date_1001", "pack_date_created"],
    )


def proccess_chunk(data, processed, chunk):
    results = []

    for idx in range(0, len(data)):
        if idx % MOD == chunk:
            shipment_id = data.iloc[idx][0]
            pack_id = data.iloc[idx][1]

            try:
                if not shipment_id in processed:

                    checkpoint_date_7000, checkpoint_date_1001, pack_date_created = process(shipment_id, pack_id)

                    results.append(
                        {
                            "shipment_id": shipment_id,
                            "pack_id": pack_id,
                            "checkpoint_date_7000": checkpoint_date_7000,
                            "checkpoint_date_1001": checkpoint_date_1001,
                            "pack_date_created": pack_date_created,
                        }
                    )

                    print(f"shipment {shipment_id} success")
                else:
                    print(f"shipment {shipment_id} skipped")

            except Exception as exception:
                print(f"shipment {shipment_id} error")
                print(exception)

        if len(results) >= BLOCK:
            persist_results(results)
            results = []

    if len(results) > 0:
        persist_results(results)


refresh_token()
data = pandas.read_csv("source.csv")
processed = []

for idx, row in pandas.read_csv("output.csv").iterrows():
    processed.append(row.shipment_id)
multiprocessing.set_start_method("fork")
pool = multiprocessing.Pool(processes=MOD + 1)
pool.apply_async(refresh_token_cron)

for i in range(0, MOD):
    pool.apply_async(proccess_chunk, (data, processed, i))
pool.close()
pool.join()
