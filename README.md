# NBA Score Predictor Using TensorFlow
This repository is a tutorial project for using NBA box score stats to predict the outcomes of games. 
The code for this project is written in Python and uses TensorFlow for models.
This project is a full-scale, end-to-end pipeline that covers:
- Scraping box score data from the internet
- Wrangling data into a usable format
- Designing and utilizing a local SQL database to store and retrieve data
- Creating multiple datasets with different input features to assess the effect of complexity on performance
- Building and testing neural network models for each complexity level
- Performing statistical tests to assess the difference in performance between models
- Visualizing fit of models

This project served as a way for me to apply the knowledge I gained from university to a real world problem that interested me and also helped me pick up many new skills and tools along the way.
I have made the repository public with hopes of helping anyone else who might be interested in learning applied data science and machine learning. 
I did my best to comment and document my code so that anyone who reads it knows what the thought process and reasoning is behind a block of code.

While I do create example datasets and models, I encourage you to play around with these. 
There are so many different possible datasets and models that could be created, it is highly unlikely that the ones I made are the best.
Try different things, see what works, what doesn't, and try to determine why it behaves the way it does. 
That is what data science is all about.

## Project Structure
This project is split into three distinct phases: 
- [Scraping, Wrangling, and Databasing](#scraping-wrangling-and-databasing)
- [Dataset Creation](#dataset-creation)
- [Model Building and Evaluation](#model-building-and-evaluation)

### Scraping, Wrangling, and Databasing
The stats used by this project are all scraped from Basketball Reference. 
I chose to scrape games beginning in 2000, but this can changed as long as all the basic and advanced box score stats are available for every game.
All scrapers are stored in the `/Scrapers` directory.
First, links to box scores are scraped for each day of the year using `scrape_box_score_links.py`. 
This process takes around 5-6 hours because of IP rate limiting of 30 requests per second.

Next, I set up a database to store all the box score information. 
My code assumes you use a PostgreSQL database. Any other type of database will not work.
My code also assumes you store the connection parameters to your database in a file named `database_helpers.py`.
The expected structure of these parameters can be found in [Appendix A](#a-postgresql-connection-parameter-dictionary-structure).
All database scripts are stored in the `/Database Scripts` directory. 
The scripts to create the tables can be found in `create_tables.sql`.
I want to note here that my database design is not ideal, but my focus was on getting something working.
The biggest issue is that I used players' names as the primary key without knowing Basketball Reference provides unique identifiers for each player in an HTML attribute. 
This means players who share the same first and last name cause problems, more on that later.
I also created indexes for my tables in `create_indexes.sql`. This should speed up SQL queries that sort by the game date.

With the links to the box scores and the database set up, `scrape_box_scores.py` visits the links and stores the data in the database.
This process takes around 22-23 hours, again because of IP rate limiting.
Both player and team stats are recorded for every box score.
If a player did not play in the game but was available to play, the "Did Not Play" text by Basketball Reference is converted to zeros for every stat.
Otherwise, the player is not added to the database because he could not have impacted the outcome of the game.
Links where the data extraction process failed are stored along with the errors associated with them. 
This allows the possibility to diagnose what the issue might be and how to solve it without completely ruining the rest of the database or having to waste another 22-23 hours.

As mentioned earlier, I ran into three error cases of players having duplicate names: Marcus Williams, Chris Johnson, Tony Mitchell. 
I resolved this issue by simply deleting all games involving those players. The script to do this is `delete_name_collisions.sql`.
This is obviously not a pretty or ideal fix, but I did not want to wait another 23 hours to get the data after changing my database structure.
Another problem I ran into was that box plus minus is not calculated for play-in games. 
I chose to ignore this error at the cost of not having play-in games in the database.
Finally, there we some instances of Basketball Reference listing a player for two teams simultaneously when the player changed teams mid-season.
This is a problem because I have a constraint on my tables that a player can only play in one game a day.
Each of these cases must be handled separately and can be found at the end of the README in [Appendix B](#b-failed-links-due-to-team-switch).

### Dataset Creation
The next phase in this project makes datasets to train models using the scraped data stored in the database.
The code and datatsets for this phase are stored in the `/Datasets` directory.
There are a practically infinite different number of datasets that could be created from the scraped data.
I have chosen a handful of sets of different input features that I have found to provide useful insights into which predictors provide the most value and how complexity impacts performance.
The input features for each level of complexity can be found in the `create_datasets.py` file. 
I encourage you to test your own sets of input features and see how they compare.
I chose to use a rolling 10 game average for each input feature for each dataset.
This window can be changed, but I found the best success with 10 games for the window lengths I tested.

### Model Building and Evaluation
The last step builds neural network models using TensorFlow and trains them using the datasets created in the previous phase.
All code for models is stored in the `/Model` directory.
The code that actually constructs the model is in `model.py` and builds a simple sequential fully-connected neural network.
The code that loads the dataset and splits into training, validation, and testing sets is located in `data.py`.
The `base.py` file is the file that actually runs the experiment.
It handles loading the dataset with splits, building the model, training the model, and saving test set results.
It accepts command line arguments that handle everything about the experiment, including file locations for the dataset and results, as well as model architecture specification.
The command line argument parser is defined in `parser.py`.
For a list of every command line option and what its function is, run the command `python Model/base.py --help`.
These command line arguments can also be specified in a .txt file that is referenced in the command line with an @ prefix.
A sample set of command line arguments is provided in [Appendix C](#c-sample-command-line-arguments).
The `evaluate.py` file loads saved model results and evaluates them in a number of ways, including statistical significance tests and residual plots.
The `predict.py` file loads a saved model and uses it to predict the score of game or list of games.
I did not save every set of arguments for every experiment I ran because there were just too many.
You can find the summary of experiments and findings in [My Results](#my-results)

## My Results

## Appendix
### A: PostgreSQL Connection Parameter Dictionary Structure
connection_parameters = {\
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;'dbname': 'your-db-name',\
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;'user': 'your-db-username',\
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;'password': 'your-db-password',\
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;'host': 'your-db-host',\
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;'port': 'your-db-port'\
}

The default username is postgres, default host is localhost, and default port is 5432.

### B: Failed Links Due to Team Switch
- /boxscores/200101030PHI.html
  - Jim Jackson, Anthony Johnson, and Larry Robinson are listed for both Cavs and Hawks
  - Ensure only Cavs game counts (they were traded)
- /boxscores/200303070SEA.html
  - Tyrone Hill is listed for both 76ers and Cavs
  - Ensure only 76ers game counts (he was traded)
- /boxscores/200304190NJN.html
  - Erick Strickland is listed for both Pacers and Bucks
  - Ensure only Pacers game counts (human error)
- /boxscores/200503010MEM.html
  - Rodney White and Nikoloz Tskitishvili are listed for both Warriors and Nuggets
  - Ensure only Warriors game counts (they were traded)
- /boxscores/200512160PHI.html
  - Matt Barnes is listed for both 76ers and Knicks
  - Ensure only 76ers game counts (he was traded)
- /boxscores/200602220HOU.html
  - Vladimir Radmanovic is listed for both Clippers and Sonics
  - Ensure only Clippers game counts (he was traded)
- /boxscores/200602270SAC.html
  - Ruben Patterson and Charles Smith are listed for both Nuggets and Blazers
  - Ensure only Nuggets game counts (they were traded)
- /boxscores/200603030PHO.html
  - Tim Thomas is listed for both Suns and Bulls
  - Ensure only Suns game counts (he was traded)
- /boxscores/200701220LAL.html
  - Al Harrington, Stephen Jackson, Josh Powell, and Šarūnas Jasikevičius are listed for Warriors and Pacers
  - Ensure only Warriors game counts (they were traded)
- /boxscores/200712190DAL.html
  - Shawn Marion and Jason Williams played in two games due to dispute
  - Get rid of disputed game
- /boxscores/200712310UTA.html
  - Kyle Korver is listed for both Jazz and 76ers
  - Ensure only Jazz game counts (he was traded)
- /boxscores/200802200NOH.html
  - Jason Kidd is listed for both Mavs and Nets
  - Ensure only Mavs game counts (he was traded)
- /boxscores/200804130LAL.html
  - Bobby Jones played for 5 teams (including the Nuggets for two separate stints)
  - Bobby Jones is listed for both Nuggets and Spurs
  - Ensure only Nuggets game counts (he was cut)
- /boxscores/200902230SAC.html
  - Chris Wilcox is listed for both Knicks and Hornets
  - Ensure only Knicks game counts (he never played for NOH)

### C: Sample Command Line Arguments
`python Model/base.py @exp.txt @model.txt`

#### exp.txt
--project=NBA-Box-Scores\
--exp_type=intermediate_2\
--dataset=./Datasets/intermediate_dataset.pkl\
--kfold\
--folds=10\
--train_folds=8\
--val_folds=1\
--test_folds=1\
--results_path=./Results\
--epochs=100\
--lrate=0.00001\
--loss=mse\
--es_min_delta=0.001\
--es_patience=10\
--es_monitor=val_loss\
-vv\
--wandb\
--predictions\
--save_model

#### model.txt
--hidden\
256\
128\
64\
--hidden_activation=elu\
--output_activation=linear