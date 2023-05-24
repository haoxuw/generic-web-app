import fastapi
import pymongo

import mongo_connection


db_instance = mongo_connection.MongoConnection.get_instance()
router = fastapi.APIRouter(prefix="/api/generic_data", tags=["generic_data"])


def filter_private(json_like):
    if isinstance(json_like, list):
        for item in json_like:
            filter_private(item)
    elif isinstance(json_like, dict):
        to_be_deleted = [key for key in json_like.keys() if key.startswith("_")]
        for key in to_be_deleted:
            del json_like[key]
        for key in json_like:
            if isinstance(json_like[key], dict):
                filter_private(json_like[key])


@router.get("/messages/")
def messages(
    collection_name: str = "monolith",
    name: str = None,
    message: str = None,
    skip: int = 0,
    limit: int = 32,
):
    filter_fields = {"name": name, "message": message}
    return versatile_query(
        collection_name,
        filter_fields=filter_fields,
        skip=skip,
        limit=limit,
    )


def generic_query_str_to_dict(generic_query_str: str):
    if generic_query_str is None:
        return None
    return {
        field_name: field_values
        for field_name, field_values in [
            generic_single_query.split(":")
            for generic_single_query in generic_query_str.split(",")
        ]
    }


# example query:
# ?collection_name=monolith
# &filter_fields_str=name:John Doe;Jean Doe,id:1;2
# &filter_ranges_str=created_at:1684899023.827678;1684899023.827680
# &sort_by_str=name:ASC
# &skip=0&limit=32
@router.get("/backdoor/in/")
def backdoor(
    collection_name: str = "monolith",
    filter_fields_str: str = None,
    filter_ranges_str: str = None,
    sort_by_str: str = None,
    skip: int = 0,
    limit: int = 32,
):
    filter_fields = generic_query_str_to_dict(filter_fields_str)
    filter_ranges = generic_query_str_to_dict(filter_ranges_str)
    sort_by = generic_query_str_to_dict(sort_by_str)
    return versatile_query(
        collection_name,
        filter_fields=filter_fields,
        filter_ranges=filter_ranges,
        sort_by=sort_by,
        skip=skip,
        limit=limit,
    )


def versatile_query(
    collection_name,
    filter_fields=None,
    filter_ranges=None,
    sort_by=None,
    skip: int = 0,
    limit: int = 32,
):
    try:
        collection = db_instance[collection_name]
    except Exception as exc:
        return {"error": "A valid collection name is required: " + str(exc)}

    aggregated_query = []

    if filter_fields is not None:
        field_filter = {}
        for query_field, query_value in filter_fields.items():
            if query_value is not None:
                field_filter[query_field] = {"$in": query_value.split(";")}
        if field_filter:
            aggregated_query += [{"$match": field_filter}]

    if filter_ranges is not None:
        range_filter = {}
        if filter_ranges:
            range_filter["$expr"] = {}
        for range_filter_field, range_filter_value in filter_ranges.items():
            if range_filter_value is not None:
                range_filter_value_pair = range_filter_value.split(";")
                assert (
                    len(range_filter_value_pair) <= 2
                ), f"Invalid range: {range_filter_value}"
                start, end = range_filter_value.split(";")
                if start is not None and end is not None:
                    if start == end:
                        range_filter["$expr"]["$eq"] = [
                            f"${range_filter_field}",
                            float(start),
                        ]
                    else:
                        range_filter["$expr"]["$and"] = [
                            {"$gte": [f"${range_filter_field}", float(start)]},
                            {"$lte": [f"${range_filter_field}", float(end)]},
                        ]
                else:
                    if start:
                        range_filter["$expr"]["$gte"] = [
                            f"${range_filter_field}",
                            float(start),
                        ]
                    else:
                        range_filter["$expr"]["$lte"] = [
                            f"${range_filter_field}",
                            float(end),
                        ]
        if range_filter:
            aggregated_query += [{"$match": range_filter}]

    count_cursor = collection.aggregate(
        aggregated_query + [{"$group": {"_id": None, "count": {"$sum": 1}}}]
    )
    count_cursor = list(count_cursor)
    if count_cursor:
        total = count_cursor[0]["count"]
    else:
        total = 0

    if sort_by:
        sorting = {}
        for sort_field, sort_order in sort_by.items():
            sorting[f"{sort_field}"] = 1 if sort_order.lower() == "asc" else -1
        aggregated_query += [{"$sort": sorting}]
        print(sorting)

    limit = min(limit, 1024)
    joined = collection.aggregate(
        [*aggregated_query, {"$skip": skip}, {"$limit": limit}]
    )
    got = mongo_connection.MongoConnection.to_serializable(cursor=joined)
    filter_private(got)
    return {"got": got, "retrieved": len(got), "total": total}
