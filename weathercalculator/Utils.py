def pairwise_union(dataframes_list):
    while len(dataframes_list) > 1:
        unified_df = [
            df1.union(df2).distinct()
            for df1, df2 in zip(dataframes_list[::2], dataframes_list[1::2])
        ]
        if len(dataframes_list) > 1 and len(unified_df) % 2 == 1:
            unified_df[-1] = unified_df[-1].union(dataframes_list[-1]).distinct()
        dataframes_list = unified_df
    return unified_df[0]
