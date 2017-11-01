###################Approach###################### 
Step1: Read data from the file one line at a time.
Step2: Check the common conditions for both problems to exclude data. For e.g. if the OTHER_ID field is present exclude the entire line.
Step3:Append the read line to the list of dictionary sequentially.
Step4:Compute median value of amounts by zip sequentially for each input.
Step6:Output the result to file sequentially.
Step5:Problem 1 done after reading and processing last line in the file.
Step6:You now have a list with all records stored as dictionary.
Step7:Group the entire list by CMTE_ID and DATE 
Step8:Find median of amount for each unique combination of DATE and CMTE_ID.
Step9:write the results to the file.
################### Code written on ####################
python 3
packages:io, math, datetime, sys,collections[defaultdict]


