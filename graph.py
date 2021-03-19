import mysql.connector
import matplotlib.pyplot as plt

db = mysql.connector.connect(
    user="root",
    password="",
    database="bachelor"
)
cursor = db.cursor()


def get_data():
    cursor.execute(f"SELECT articles.Id, outlet, polarity, compound FROM articles, textblob, vader "
                   f"WHERE vader.articleId = articles.id AND vader.articleId = textblob.articleId AND textblob.articleId = articles.id LIMIT {92785 + 304435}")
    query_results = cursor.fetchall()
    mapping = dict()
    for result in query_results:
        outlet = result[1]
        id = result[0]
        if outlet not in mapping:
            mapping[outlet] = {
                'tb': dict(),
                'vader': dict()
            }
        mapping[outlet]['tb'][id] = result[2]
        mapping[outlet]['vader'][id] = (result[2])
    return mapping


def plot(mapping):
    num_of_outlets = len(mapping.keys())
    fig = plt.figure()
    axis = fig.subplots(num_of_outlets, 2)
    axis[0, 0].set_title('Vader')
    axis[0, 1].set_title('TextBlob')
    for i, (outlet, analysisTool_dict) in enumerate(mapping.items()):
        axis[i, 0].scatter(list(range(len(analysisTool_dict['vader'].keys()))), analysisTool_dict['vader'].values(), s=0.1, c='b', marker='o', label='vader')
        axis[i, 0].set_ylabel(outlet)
        axis[i, 1].scatter(list(range(len(analysisTool_dict['tb'].keys()))), analysisTool_dict['tb'].values(), s=0.1, c='r', marker='o', label='textblob')
    fig.tight_layout()
    plt.show()


def add_subplot(axis, title, color, label, index, x, y):
    axis[index].scatter(x, y, s=10, c=color, marker='o', label=label)
    axis[index].set_title(title)


if __name__ == '__main__':
    print("Lets fuck shit up!")
    mapping = get_data()
    plot(mapping)

