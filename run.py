import pandas, requests, json

# The main Idea:
# if must_be_fixed:
# we get sibling_source (check if null)
# if sibling_source is split we should remove partial cancellation
# if sibling_source is partial cancellation we should remove source_splitter_legacy and source_pack_split
# if sibling_source is null return/print error message

# Curls used to build the script:

#   curl --location --request GET 'https://internal-api.mercadolibre.com/shipments/41449531583?caller.scopes=admin' \
# --header 'x-format-new: true' \
# --header 'X-Auth-Token: xxxxxx' \
# --data-raw ''

# curl --location --request DELETE 'https://internal-api.mercadolibre.com/shipments/41449531583/tags/source_splitter_legacy?caller.scopes=admin' \
# --header 'X-Auth-Token: xxxxxx' \
# --data-raw ''


TOKEN = "TOKEN UNDEFINED"


def must_be_fixed(tags):
    return ("source_partial_cancellation" in tags and "source_pack_split" in tags) or (
        "source_partial_cancellation" in tags and "source_splitter_legacy" in tags
    )


def delete_tag(tag_to_remove, shipment_id):
    response = requests.delete(
        f"https://internal-api.mercadolibre.com/shipments/{shipment_id}/tags/{tag_to_remove}?caller.scopes=admin",
        headers={"x-auth-token": TOKEN},
    )
    return response.status_code in [204, 404]


def process(shipment_id: str):
    shipment = requests.get(
        f"https://internal-api.mercadolibre.com/shipments/{shipment_id}?caller.scopes=admin",
        headers={"x-auth-token": TOKEN, "x-format-new": "true"},
    )

    return_message = "Undefined"

    if shipment.status_code == 200:
        shipment = json.loads(shipment.text)
        tags = shipment["tags"]
        if must_be_fixed(tags):
            if "sibling" in shipment and "source" in shipment["sibling"]:
                sibling_source = shipment["sibling"]["source"]
                if sibling_source is not None:
                    if "split" in sibling_source:
                        # we should remove partial cancellation and return result message
                        delete_was_ok = delete_tag("source_partial_cancellation", shipment_id)
                        return_message = "OK" if delete_was_ok else "Error deleting tag source_partial_cancellation"
                    else:
                        if "partial_cancellation" in sibling_source:
                            # we should remove source_splitter_legacy and source_pack_split
                            delete_was_ok = delete_tag("source_pack_split", shipment_id)
                            delete_legacy_was_ok = delete_tag("source_splitter_legacy", shipment_id)
                            return_message = (
                                "OK" if delete_was_ok and delete_legacy_was_ok else "Error deleting split tag"
                            )
                else:
                    return_message = "sibling_source not split nor partial_cancellation"
            else:
                return_message = "Sibling or source not in shipment"
        else:
            return_message = "No double tags"
    else:
        return_message = "Shipment not found "
    return return_message


data = pandas.read_csv("input.csv")

for _, row in data.iterrows():
    process_response = process(row.shipment_id)
    print(f"{row.shipment_id}\t" + process_response)
