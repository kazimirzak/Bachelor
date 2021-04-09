import math
import time

import matplotlib.pyplot as plt
from pymongo import MongoClient
from dateutil import parser

client = MongoClient('mongodb://localhost:27017')
db = client.Bachelor


def get_data(pipeline):
    # Expects the pipeline to be a pipelines used to aggregate a query in mongodb.
    # The resulting rows must contain '_id', 'vader', 'tb'
    # Where '_id' = labeling of said data, 'vader' = vader sentiment of the data
    # 'tb' = textblob sentiment of the data.
    print("---   Querying database   ---")
    start_time = time.time()
    data = db.Articles.aggregate(pipeline, allowDiskUse=True)
    print("--- Done in: %.2f seconds ---" % (time.time() - start_time))
    return create_mapping(data)


def create_mapping(data):
    mapping = dict()
    for item in data:
        mapping[item['_id']] = {
            'Vader': item['Vader'],
            'TextBlob': item['TextBlob']
        }
    return mapping


def inverse_mapping(mapping):
    newMapping = dict()
    for outlet, sentiment_dict in mapping.items():
        for sentiment_tool, l in sentiment_dict.items():
            if sentiment_tool not in newMapping:
                newMapping[sentiment_tool] = dict()
            newMapping[sentiment_tool][outlet] = l
    return newMapping


def plot_1d(mapping, num_rows, num_columns):
    fig = plt.figure()
    axis = fig.subplots(ncols=num_columns, nrows=num_rows)
    for i, (label, sentiment_dict) in enumerate(mapping.items()):
        keys = [x for x in sentiment_dict.keys() if x != "_id"]
        axis[i].set_xticks(range(1, len(keys) + 1))
        axis[i].set_xticklabels(keys)
        axis[i].set_title(label)
        axis[i].violinplot([sentiment_dict[x] for x in keys])
    plt.show()


def plot_2d(mapping, num_rows, num_columns):
    fig = plt.figure()
    axis = fig.subplots(ncols=num_columns, nrows=num_rows)
    for i, (label, sentiment_dict) in enumerate(mapping.items()):
        row = i // num_columns
        col = i % num_columns
        keys = [x for x in sentiment_dict.keys() if x != "_id"]
        axis[row, col].set_xticks(range(1, len(keys) + 1))
        axis[row, col].set_xticklabels(keys)
        axis[row, col].set_title(label)
        axis[row, col].violinplot([sentiment_dict[x] for x in keys], showmeans=True, showmedians=True)
    plt.show()


if __name__ == '__main__':
    print("Lets fuck shit up!")
    user_input = "a"
    mapping = None
    correctInput = False
    pipeline = None
    while user_input != "q":
        user_input = input(
            "Please choose from the list below what the data should be grouped by:\n1: Outlet\n2: Year\n3: Month\n4: Day\nQ: Exit\n").lower()
        if user_input == "1":
            correctInput = True
            pipeline = [
                {
                    "$group":
                        {
                            "_id": "$uniqueId",
                            "label": {"$first": "$outlet"},
                            "vader": {"$first": "$vader"},
                            "tb": {"$first": "$tb"}
                        }
                },
                {
                    "$group":
                        {
                            "_id": "$label",
                            "Vader": {"$push": "$vader"},
                            "TextBlob": {"$push": "$tb"}
                        }
                },
                {
                    "$sort":
                        {
                            "_id": 1
                        }
                }
            ]
        if user_input == "2":
            correctInput = True
            pipeline = [
                {
                    "$group":
                        {
                            "_id": {"$year": "$date"},
                            "Vader": {"$push": "$vader"},
                            "TextBlob": {"$push": "$tb"}
                        }
                },
                {
                    "$sort":
                        {
                            "_id": 1
                        }
                }
            ]
        if user_input == "3":
            correctInput = True
            pipeline = [
                {
                    "$group":
                        {
                            "_id": {
                                "year": {"$year": "$date"},
                                "month": {"$month": "$date"}

                            },
                            "vader": {"$push": "$vader"},
                            "tb": {"$push": "$tb"}
                        }
                },
                {
                  "$sort":
                      {
                          "_id": 1
                      }
                },
                {
                    "$project":
                        {
                            "_id": {
                                "$concat": [
                                    {"$toString": "$_id.month"},
                                    "/",
                                    {"$toString": "$_id.year"}
                                ]
                            },
                            "Vader": "$vader",
                            "TextBlob": "$tb"
                        }
                }
            ]
        if user_input == "4":
            correctInput = False
            minDate = db.Articles.find_one(sort=[("date", 1)])["date"]
            maxDate = db.Articles.find_one(sort=[("date", -1)])["date"]
            print(f"Please enter start and end dates between the interval: {minDate.strftime('%x')}-{maxDate.strftime('%x')} ")
            startDate = parser.parse(input("Start Date: "))
            endDate = parser.parse(input("End Date: "))
            if minDate < startDate < endDate < maxDate:
                correctInput = True
            else:
                print("Wrong dates, skid!")
            pipeline = [
                {
                    "$group":
                        {
                            "_id": {
                                "year": {"$year": "$date"},
                                "month": {"$month": "$date"},
                                "day": {"$dayOfMonth": "$date"}

                            },
                            "vader": {"$push": "$vader"},
                            "tb": {"$push": "$tb"}
                        }
                },
                {
                    "$match":
                        {
                            "_id.year": {"$gte": startDate.year, "$lte": endDate.year},
                            "_id.month": {"$gte": startDate.month, "$lte": endDate.month},
                            "_id.day": {"$gte": startDate.day, "$lte": endDate.day}
                        }
                },
                {
                    "$sort":
                        {
                            "_id": 1
                        }
                },
                {
                    "$project":
                        {
                            "_id": {
                                "$concat": [
                                    {"$toString": "$_id.day"},
                                    "/",
                                    {"$toString": "$_id.month"},
                                    "/",
                                    {"$toString": "$_id.year"}
                                ]
                            },
                            "Vader": "$vader",
                            "TextBlob": "$tb"
                        }
                }
            ]
        if correctInput:
            mapping = get_data(pipeline)
            if input("Inverse Mapping? ") == "y":
                mapping = inverse_mapping(mapping)
            for key, value in mapping.items():
                print(key, "->", value.keys())
            num_columns = math.floor(math.sqrt(len(mapping)))
            num_rows = math.ceil(len(mapping) / num_columns)
            if num_rows == 1 or num_columns == 1:
                plot_1d(mapping, num_rows, num_columns)
            else:
                plot_2d(mapping, num_rows, num_columns)
        correctInput = False
