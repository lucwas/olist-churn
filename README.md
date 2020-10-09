# olist-churn
Predicting churn in a real company for CRM actions. SQL database accessing, ML modelling, automatic features and simple deploy.

## Company & Data
The data used in this project comes from [this dataset](https://www.kaggle.com/olistbr/brazilian-ecommerce), from Kaggle.
Olist is a Brazilian startup that provides services for marketplaces sellers.
They offer indicators, concurrence analysis, automatic categorization, ship tags generation, among other benefits.

The data shows the historic of all products sold by their clients (the marketplace sellers) from Sep '16 to Sep '18. The data are transformed in a .db file (from which the csv from the dataset came from) for simulating a real life project accessing a cloud with the data.

Consider that this .db file is located at <code>/database</code> folder (it was not uploaded due to its size), but this file could be in a cloud, for example.

## The Project
The aim of this project is to predict the sellers who will churn - considered here as not selling anything for 3 months (probably leaving the platform or abandoning the activity). At each point in time, for each seller in the active base, it is given a probability of completing the 3-month-period of inactivity.

These results are deployed as a Table which would be consulted by the CRM Team for the appropriate measures (as decreasing the percentage of the sells taxes, highlighting them at the platforms or making contact). The database fraction analysed was the year of 2017.

## Book of Variables
The main variables created are:
- Sellers' summarized products values (average ticket, total receipt, number of sells);
- Recency of last sell;
- Percentage of days with sells;
- Geographic location of clients;
- Category of sold products; and
- Payment types.

## Machine Learning Model
The chosen model is a LightGBoost with RandomSearch tuned parameters.

## Results
It was observed that is expected a loss of R$ 1.6MM/year of sellers activity. Approaching 32% of the active base of sellers, 87% of the churned sellers could be identified. If retained, these sellers would represent a potential receipt of R$ 1.32MM/year.

## Presentation
A draft of a business presentation can be found at <code>crm-churn/docs</code>.

## Automatic running
For updating the models for a recent date, it can be run in a bash/shell the following commands (considering a set environment):

<pre>
<code>python olist_book/run.py --exec insert --date '2018-01-02'</code>
<code>python olist_book/src/data_prep/predict/get_predict.py --date '2018-01-02'</code>
<code>python olist_book/src/ml/predict/predict.py</code>
<code>python olist_book/src/ml/predict/upload.py</code>
</pre>

where '2018-01-02' must be replaced by the current date. This can be set to be automatically run with the desired frequency (considering a process of follow-up for continuous model validation over time).