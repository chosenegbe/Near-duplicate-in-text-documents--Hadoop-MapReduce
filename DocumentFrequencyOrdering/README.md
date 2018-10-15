The first mapper takes as input the documents collection and emits the individual word in the collection and the number of documents the word occurs. The second mapper takes as input the output of Reduce1 and emits the total document ferquency ordering dictionary.
Given two documents P and Q with 
P = {It is raining in Sain’t Peterburg today, am not prepared for this rain Paul }
Q= {saw the weather forecast for peterburg today, I took my umbrella, I am prepared to rock the rain}.

The program transform the tokens in the doc to:-

P = {rain saint peterburg today prepar rain paul}
Q = {weather forecast peterburg today umbrella prepar rock rain}
		Table 1. Reduce1 output of inputs P, Q
forecast 1  paul 1   perterburg 1   prepar 2   rain 2	rock 1	 saint 1	 today 2  umbrella 1	weather 1
		Table 2. Reduce2 output
forecast  paul  perterburg  rock  saint   umbrella	weather   prepare   rain   today
 
