from pymongo import MongoClient
import time
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import datetime


sns.set(rc={'figure.figsize': (11, 4)})
#sns.set_style('ticks')


if __name__ == '__main__':
    print("Lets fuck shit up")
    files = {
        'covid_ratio.csv': {
            'title': 'Volume of Covid articles',
            'xlabel': '',
            'ylabel': 'Ratio'
        },
        'vaccine_ratio.csv': {
            'title': 'Volume of vaccine articles',
            'xlabel': '',
            'ylabel': 'Ratio'
        }
    }
    for file, options in files.items():
        df = pd.read_csv(file)
        df.sort_values(by=['Month-Year'])
        fig, axes = plt.subplots()
        ax = sns.boxplot(x='Month-Year', y='Ratio', data=df, ax=axes, color='w', showfliers=False)
        sns.stripplot(x='Month-Year', y='Ratio', hue='Country', data=df, ax=axes, size=7)
        ax.set_title(options['title'])
        ax.set_xlabel(options['xlabel'])
        ax.set_ylabel(options['ylabel'])
        plt.setp(ax.artists, edgecolor='black', facecolor='w')
        plt.setp(ax.lines, color='black')
        plt.show()