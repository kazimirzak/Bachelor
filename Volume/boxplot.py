import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


plt.rcParams.update({
    "text.usetex": True,
    "font.family": "sans-serif",
    "font.sans-serif": ["Helvetica"]})
sns.set(rc={'font.size': 10, 'axes.titlesize': 16, 'axes.labelsize': 10, 'figure.figsize': (11.7, 8.27)})
sns.set(font_scale=1.25)
sns.set_style('ticks')


if __name__ == '__main__':
    print("Lets fuck shit up")
    files = {
        'covid_ratio.csv': {
            'title': 'Volume of Covid articles 2020-2021',
            'xlabel': '',
            'ylabel': 'Ratio',
            'rotate': True,
        },
        'vaccine_ratio.csv': {
            'title': 'Volume of vaccine articles 2020-2021',
            'xlabel': '',
            'ylabel': 'Ratio',
            'rotate': True
        },
        'vaccine_ratio_pre2020.csv': {
            'title': 'Volume of vaccine articles pre-2020',
            'xlabel': '',
            'ylabel': 'Ratio',
            'rotate': True
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
        if options['rotate']:
            plt.xticks(rotation=90)
        plt.savefig('E:\\bachelor\\dataIsButyful\\OfficialPics\\' + file + '.png', bbox_inches='tight')
