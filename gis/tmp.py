import pandas as pd

if __name__ == "__main__":
    df = pd.read_csv("/home/shengjh/Desktop/res.txt", delimiter=' ', header=None, names=['a', 'v'])
    r = df['v'].groupby(df['a'])
    e = r.sum()
    print(df['v'].groupby(df['a']).mean())
    print((e[1] - e[0]) / 100.0)
