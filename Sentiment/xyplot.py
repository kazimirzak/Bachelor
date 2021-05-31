import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

plt.rcParams.update({
    "text.usetex": True,
    "font.family": "sans-serif",
    "font.sans-serif": ["Helvetica"]})
sns.set(rc={'font.size': 10, 'axes.titlesize': 16, 'axes.labelsize': 10, 'figure.figsize': (11.7, 8.27)})
sns.set(font_scale=1.25)
sns.set_style('ticks')

# 21
NUM_OF_POINTS = 1001
ACCUMULATIVE = True

if __name__ == '__main__':
    print("Lets fuck shit up")
    df = pd.read_csv('is_vaccine_brands.csv', index_col=[0])
    intervals = np.linspace(-1, 1, NUM_OF_POINTS)
    mapping = dict()
    for index, row in df.iterrows():
        brand = row['Brand']
        if brand not in mapping:
            mapping[brand] = dict()
            mapping[brand]['Count'] = 0
            for i in range(len(intervals)):
                mapping[brand][intervals[i]] = 0
        mapping[brand]['Count'] += 1
        for i in range(len(intervals) - 1):
            if intervals[i] <= row['Vader'] < intervals[i + 1]:
                mapping[brand][intervals[i]] += 1
    if ACCUMULATIVE:
        for brand, count in mapping.items():
            for i in range(1, len(intervals) - 1):
                count[intervals[i]] = count[intervals[i - 1]] + count[intervals[i]]
    for brand, count in mapping.items():
        count[intervals[-1]] = count[intervals[-2]]
    brands = []
    vader = []
    ratio = []
    for brand, count in mapping.items():
        for i in range(len(intervals)):
            brands.append(brand)
            vader.append(intervals[i])
            ratio.append(count[intervals[i]] / count['Count'])

    dataframe = pd.DataFrame(data={'Brand': brands, 'Vader': vader, 'Ratio': ratio})
    dataframe.set_index('Vader')
    fig, axes = plt.subplots()
    ax = sns.lineplot(x='Vader', y='Ratio', hue='Brand', data=dataframe, ax=axes)
    ax.legend(title='Vaccines', labels=[f'{brand} (N={count["Count"]})' for brand, count in mapping.items()])
    ax.set_title('Sentiment of Vaccines')
    ax.set_ylabel('Ratio')
    ax.set_xlabel('Vader compound score')
    plt.savefig('E:\\bachelor\\dataIsButyful\\OfficialPics\\sentiment.png', bbox_inches='tight')

