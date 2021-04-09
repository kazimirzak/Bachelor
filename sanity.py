from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob

vader = SentimentIntensityAnalyzer()

text = "U.S. Army's Bergdahl suffered some of the worst abuse of any POW: expert| Reuters U.S. Army Sergeant Bowe Bergdahl endured some of the worst abuse any U.S. prisoner of war has seen for decades, a defense official said on Friday, while another added the soldier should not be imprisoned for leaving his post in Afghanistan."








print("Vader:", vader.polarity_scores(text)['compound'])
print("Tb:", TextBlob(text).sentiment.polarity)