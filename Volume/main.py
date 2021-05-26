from pymongo import MongoClient
import time
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import datetime

client = MongoClient('mongodb://localhost:27017')
db = client.Bachelor
sns.set(rc={'figure.figsize': (11, 4)})


def get_data(pipeline):
    # Expects the pipeline to be a pipelines used to aggregate a query in mongodb.
    # The resulting rows must contain '_id', 'vader', 'tb'
    # Where '_id' = labeling of said data, 'vader' = vader sentiment of the data
    # 'tb' = textblob sentiment of the data.
    print("---   Querying database   ---")
    start_time = time.time()
    data = db.Konrad.aggregate(pipeline, allowDiskUse=True)
    print("--- Done in: %.2f seconds ---" % (time.time() - start_time))
    return data


if __name__ == '__main__':
    print("lets fuck shit up!")
    num_of_covid_day = [
        {
            '$project': {
                '_id': 0,
                'uniqueId': 1,
                'year': {
                    '$year': '$date'
                },
                'month': {
                    '$month': '$date'
                },
                'country': 1,
                'outlet': 1,
                'is_covid': 1
            }
        }, {
            '$match': {
                'year': {
                    '$gte': 2020
                }
            }
        }, {
            '$group': {
                '_id': {
                    'country': '$country',
                    'outlet': '$outlet',
                    'year': '$year',
                    'month': '$month',
                    'uniqueId': '$uniqueId'
                },
                'is_covid': {
                    '$first': '$is_covid'
                }
            }
        }, {
            '$group': {
                '_id': {
                    'country': '$_id.country',
                    'outlet': '$_id.outlet',
                    'year': '$_id.year',
                    'month': '$_id.month'
                },
                'num_of_covid': {
                    '$sum': {
                        '$cond': [
                            '$is_covid', 1, 0
                        ]
                    }
                },
                'num_of_articles': {
                    '$sum': 1
                }
            }
        }, {
            '$project': {
                '_id': 0,
                'country': '$_id.country',
                'outlet': '$_id.outlet',
                'year': '$_id.year',
                'month': '$_id.month',
                'ratio': {
                    '$divide': [
                        '$num_of_covid', '$num_of_articles'
                    ]
                }
            }
        }, {
            '$sort': {
                'year': 1,
                'month': 1
            }
        }
    ]
    num_of_vaccine_day = [
        {
            '$project': {
                '_id': 0,
                'uniqueId': 1,
                'year': {
                    '$year': '$date'
                },
                'month': {
                    '$month': '$date'
                },
                'country': 1,
                'outlet': 1,
                'is_vaccine': 1
            }
        }, {
            '$match': {
                'year': {
                    '$gte': 2020
                }
            }
        }, {
            '$group': {
                '_id': {
                    'country': '$country',
                    'outlet': '$outlet',
                    'year': '$year',
                    'month': '$month',
                    'uniqueId': '$uniqueId'
                },
                'is_vaccine': {
                    '$first': '$is_vaccine'
                }
            }
        }, {
            '$group': {
                '_id': {
                    'country': '$_id.country',
                    'outlet': '$_id.outlet',
                    'year': '$_id.year',
                    'month': '$_id.month'
                },
                'num_of_vaccine': {
                    '$sum': {
                        '$cond': [
                            '$is_vaccine', 1, 0
                        ]
                    }
                },
                'num_of_articles': {
                    '$sum': 1
                }
            }
        }, {
            '$project': {
                '_id': 0,
                'country': '$_id.country',
                'outlet': '$_id.outlet',
                'year': '$_id.year',
                'month': '$_id.month',
                'ratio': {
                    '$divide': [
                        '$num_of_vaccine', '$num_of_articles'
                    ]
                }
            }
        }, {
            '$sort': {
                'year': 1,
                'month': 1
            }
        }
    ]
    country = []
    outlet = []
    month_year = []
    ratio = []
    for thing in get_data(num_of_covid_day):
        country.append(thing['country'])
        outlet.append(thing['outlet'])
        month_year.append(f"{datetime.date(1900, thing['month'], 1).strftime('%b')}-{thing['year']}")
        ratio.append(thing['ratio'])
    data = {"Country": country, "Outlet": outlet, "Month-Year": month_year, "Ratio": ratio}
    df = pd.DataFrame(data=data)
    df.to_csv('covid_ratio.csv')
    #print(df)
    #fig, axes = plt.subplots()
    #sns.boxplot(x='year-month', y='Ratio', ax=axes, grid=False)
    #axes.set_title("Volume of Vaccine Articles")
    #axes.set_xlabel("Month and year")
    #axes.set_ylabel("Percentage")
    #plt.show()
    # df = df.set_index('Date')
    #plot = df.boxplot(by=zlabel, column=[ylabel], grid=False, showmeans=True)
    #plot.set_ylabel(ylabel)
    #plot.set_xlabel(zlabel)
    #plot.set_title("Coverage of vaccines in 2020")
    #plt.show()
