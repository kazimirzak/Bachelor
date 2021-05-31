import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

plt.rcParams.update({
    "text.usetex": True,
    "font.family": "sans-serif",
    "font.sans-serif": ["Helvetica"]})
sns.set(rc={'font.size': 10, 'axes.titlesize': 16, 'axes.labelsize': 10, 'figure.figsize': (11.7, 8.27)})
sns.set(font_scale=1.25)
sns.set_style('ticks')

if __name__ == '__main__':
    print("Lets fuck shit up!")
    files = {
        #'vaccine_sentiment_by_day.csv': {
        #    'title': 'Vaccine sentiment grouped by day',
        #    'xlabel': 'Date',
        #    'ylabel': 'Sentiment',
        #    'format': '%Y-%m-%d'
        #},
        #'vaccine_sentiment_by_week.csv': {
        #    'title': 'Vaccine sentiment grouped by week',
        #    'xlabel': 'Date',
        #    'ylabel': 'Sentiment',
        #    'format': '%w-%W-%Y'
        #},
        'vaccine_sentiment_by_month.csv': {
            'title': 'Vaccine sentiment grouped by month',
            'xlabel': 'Date',
            'ylabel': 'Sentiment',
            'format': '%m-%Y'
        }
    }
    for file, options in files.items():
        df = pd.read_csv(file, index_col=[0])
        df['Period'] = pd.to_datetime(df['Period'], format=options['format'])
        fig, axes = plt.subplots()
        ax = sns.lineplot(x='Period', y='Vader', data=df, ax=axes)
        ax.set_title(options['title'])
        ax.set_xlabel(options['xlabel'])
        ax.set_ylabel(options['ylabel'])
        plt.savefig('E:\\bachelor\\dataIsButyful\\OfficialPics\\' + file + '.png', bbox_inches='tight')

