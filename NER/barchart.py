import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import math

plt.rcParams.update({
    "text.usetex": True,
    "font.family": "sans-serif",
    "font.sans-serif": ["Helvetica"]})
sns.set(rc={'font.size': 10, 'axes.titlesize': 10, 'axes.labelsize': 10, 'figure.figsize': (11.7, 8.27)})
sns.set(font_scale=1.25)
sns.set_style('ticks')


def plot_top_30(df, title, hue=False):
    fig, axes = plt.subplots()
    df = df.iloc[:30]
    if hue:
        #hue_order = ['CARDINAL', 'DATE', 'EVENT', 'FAC', 'GPE', 'LANGUAGE', 'LAW', 'LOC', 'MONEY', 'NORP', 'ORDINAL', 'ORG', 'PERCENT', 'PERSON', 'PRODUCT', 'QUANTITY', 'TIME', 'WORK_OF_ART']
        ax = sns.barplot(x='entity', y='occurrences', hue='Label', data=df, ax=axes, dodge=False)
    else:
        ax = sns.barplot(x='entity', y='occurrences', data=df, ax=axes, dodge=False)
    height_mapping = dict()
    for p in ax.patches:
        if not math.isnan(p.get_height()):
            if p.get_x() not in height_mapping:
                height_mapping[p.get_x()] = []
            height_mapping[p.get_x()].append(p.get_height())
    for i in height_mapping.values():
        i.sort()
    calculated_height = dict()
    for k, v in height_mapping.items():
        calculated_height[(k, v[0])] = v[0]
        for i in range(1, len(v)):
            val = v[i]
            for j in range(i):
                val = val - v[j]
            calculated_height[(k, v[i])] = val
    for p in ax.patches:
        if (p.get_x(), p.get_height()) in calculated_height:
            offset = -(len(str(calculated_height[(p.get_x(), p.get_height())])) * 4)
            # Uncomment if you wanna redo graph for either vaccine person or covid person, cba trying to figure out
            # how to get the to work dynamically.
            #if p.get_height() <= abs(offset) * fig.dpi * 0.25: # VACCINE PERSON
            #if p.get_height() <= abs(offset) * fig.dpi * 4: # COVID PERSON
            #    offset = offset * -1
            ax.annotate(format(calculated_height[(p.get_x(), p.get_height())], '.0f'),
                        (p.get_x() + p.get_width() / 2., p.get_height()),
                        ha='center',
                        va='center',
                        xytext=(0, offset),
                        textcoords='offset pixels',
                        rotation=90)
    ax.set_title(title)
    ax.set_ylabel('Quantity')
    ax.set_xlabel('')
    plt.xticks(rotation=90)
    #plt.show()
    plt.savefig('E:\\bachelor\\dataIsButyful\\OfficialPics\\' + title + '.png', bbox_inches='tight')


def plot_top_30_org(df, title):
    plot_top_30(df.loc[df['Label'] == ' ORG'], title)


def plot_top_30_person(df, title):
    plot_top_30(df.loc[df['Label'] == ' PERSON'], title)


def plot_top_30_event(df, title):
    plot_top_30(df.loc[df['Label'] == ' EVENT'], title)


def plot_events_by_country(label):
    ratio = False
    if ratio:
        formater = lambda x: format(x * 100, '.0f') + '%'
    else:
        formater = lambda x: format(x, '.0f')
    path = f'./{label}/by_country/'
    counts = []
    countries = []
    for f in os.listdir(f'./{label}/by_country'):
        df = pd.read_csv(path + f, index_col=[0])
        if ratio:
            num_of_event_labels = len(df.loc[(df['label'] == 'EVENT')])
        else:
            num_of_event_labels = 1
        df = df.loc[(df['label'] == 'EVENT') & ((df['entity'] == 'World War II') | (df['entity'] == 'WWII'))]
        country = f.split('_')[-1].replace('.csv', '')
        count = df['occurrences'].sum() / num_of_event_labels
        counts.append(count)
        countries.append(country)
    df = pd.DataFrame(data={"Country": countries, 'Occurrences': counts})
    df = df.sort_values(by=['Occurrences'], ascending=False)
    fig, axes = plt.subplots()
    ax = sns.barplot(x='Country', y='Occurrences', data=df, ax=axes, dodge=False)
    for p in ax.patches:
        ax.annotate(formater(p.get_height()),
                    (p.get_x() + p.get_width() / 2., p.get_height()),
                    ha='center',
                    va='center',
                    xytext=(0, 9),
                    textcoords='offset points')
    title = 'Number of "World War II" and "WWII" EVENT labels by country for '
    if label == 'is_vaccine':
        title += 'vaccine'
    elif label == 'is_covid':
        title += 'Covid'
    title += ' articles'
    ax.set_title(title)
    ax.set_xlabel('Country')
    ax.set_ylabel('Quantity')
    plt.savefig('E:\\bachelor\\dataIsButyful\\OfficialPics\\WWII' + label + '.png', bbox_inches='tight')


if __name__ == '__main__':
    print("lets fuck shit up!")
    vaccine = pd.read_csv("is_vaccine/is_vaccine_entities.csv")
    vaccine.rename(columns={'label': 'Label'}, inplace=True)
    covid = pd.read_csv("is_covid/is_covid_entities.csv")
    covid.rename(columns={'label': 'Label'}, inplace=True)
    plot_top_30(vaccine, 'Named entities for vaccine articles', True)
    plot_top_30(covid, 'Named entities for Covid articles', True)
    plot_top_30_org(vaccine, 'Organization entities for vaccine articles')
    plot_top_30_org(covid, 'Organization entities for Covid articles')
    #plot_top_30_person(vaccine, 'Person entities for vaccine articles')
    #plot_top_30_person(covid, 'Person entities for Covid articles')
    plot_top_30_event(vaccine, 'Event entities for vaccine articles')
    plot_top_30_event(covid, 'Event entities for Covid Articles')
    plot_events_by_country('is_vaccine')
    plot_events_by_country('is_covid')
