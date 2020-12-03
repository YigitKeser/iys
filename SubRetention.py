import pyodbc
import pandas as pd
from operator import attrgetter
from matplotlib import pyplot as plt
import seaborn as sns
import matplotlib.colors as mcolors


def get_data(source):
    if source != 'all':
        q = """SELECT * FROM [#].[dbo].[vw_retention] where ParentSource = '{}'""".format(source)
    if source == 'DRTV':
        q = """SELECT * FROM [#].[dbo].[vw_retention] where ChildSource = '{}'""".format(source)
    else:
        q = "SELECT * FROM [#].[dbo].[vw_retention]"
    conn_str = "FILEDSN=mssql1.dsn;Trusted_Connection=yes"
    conn = pyodbc.connect(conn_str)
    df = pd.read_sql(q, conn)
    return df


def prepare_data(df):
    df["PaymentDate"] = pd.to_datetime(df["PaymentDate"])
    df['PaymentDate'] = df['PaymentDate'].dt.to_period('M')
    df = df[['PersonID', 'PaymentDate']].drop_duplicates()
    return df


def cohort(df, source):
    df['cohort'] = df.groupby('PersonID')['PaymentDate'].transform('min')
    df_cohort = df.groupby(['cohort', 'PaymentDate']) \
        .agg({"PersonID": pd.Series.nunique}) \
        .reset_index(drop=False)
    df_cohort['period_number'] = (df_cohort.PaymentDate - df_cohort.cohort).apply(attrgetter('n'))
    df_cohort.rename(columns={'PersonID': 'PersonCount'}, inplace=True)
    cohort_pivot = df_cohort.pivot_table(index='cohort',
                                         columns='period_number',
                                         values='PersonCount')

    cohort_size = cohort_pivot.iloc[:, 0]
    retention_matrix = round(cohort_pivot.divide(cohort_size, axis=0), 2)
    retention_matrix.to_csv('Retention' + source + '.csv')
    return cohort_size, retention_matrix


def plot(cohort_size, retention_matrix, source):
    with sns.axes_style("white"):
        fig, ax = plt.subplots(1, 2, figsize=(24, 16), sharey=True, gridspec_kw={'width_ratios': [1, 11]})

    # retention matrix
    sns.heatmap(retention_matrix,
                mask=retention_matrix.isnull(),
                annot=True,
                fmt='.0%',
                cmap='RdYlGn',
                ax=ax[1])
    ax[1].set_title('Monthly Cohorts: Payment Retention', fontsize=16)
    ax[1].set(xlabel='# of periods',
              ylabel='')

    # cohort size
    cohort_size_df = pd.DataFrame(cohort_size).rename(columns={0: 'cohort_size'})
    white_cmap = mcolors.ListedColormap(['white'])
    sns.heatmap(cohort_size_df,
                annot=True,
                cbar=False,
                fmt='g',
                cmap=white_cmap,
                ax=ax[0])

    fig.tight_layout()
    fig.savefig('Retention_' + source + '.png', dpi=350)


def main(source):
    df = get_data(source)
    df = prepare_data(df)
    cohort_size, retention_matrix = cohort(df, source)
    plot(cohort_size, retention_matrix, source)


if __name__ == "__main__":
    source = ['DD', 'Online', 'Lead', 'DRTV']
    for row in source:
        main(source)

