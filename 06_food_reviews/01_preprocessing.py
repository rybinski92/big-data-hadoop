import pandas as pd

df = pd.read_csv('Reviews.csv')

df.to_csv('prep_reviews.tsv', sep='\t', header=False, index=False)


