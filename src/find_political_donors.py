import io
import math
import datetime
import sys
import time
#Declare a list to hold data
donarData = []
#------------Methods---------------------------------------------
#Method to compute Median
def findMedian(data):
    data=sorted(data)
    length=len(data)
    if (length==1):
        return data[0]
    elif(length%2==0):
        return round(((float)(data[int(length/2)])+(float)(data[int(length/2)-1]))/2)
    else:
        return (data[int(length/2)])

#Method to check whether the date is valid or not
def validateDate(date_text):
    correctDate = None
    try:
        newDate = datetime.datetime.strptime(date_text, '%m%d%Y')
        correctDate = True
    except ValueError:
        correctDate = False
    return correctDate
#Method to convert string date to  date object
def convertToDateObject(date_text):
    try:
        newDate = datetime.datetime.strptime(date_text, '%m%d%Y')
    except ValueError:
       raise ValueError('verify the date before conversion')
    return newDate
#Method to convert the date object to string date
def convertDateObjectToString(date_object):
    try:
        newDate=date_object.strftime('%m%d%Y')
    except ValueError:
       raise ValueError('verify the date before conversion')
    return newDate

#---------------------------------------------------------------------------------------------------
#Method to process the Data (This method is called from main)
#---------------------------------------------------------------------------------------------------

def processData(inputFileName,outputFileNameforMedianValuebyZip,outputFileNameforMedianValuebyTime):
    file  = open(outputFileNameforMedianValuebyZip,"w")
    #------------------------Reading Input sequentially to simulate streaming-----------------------
    # Reading input file
    with io.open(inputFileName,\
    'r', encoding='latin-1') as infile:
        for line in infile:  # looping through the lines
           splitted=line.split('|') #Split the data
           #Clean the data for Analysis purpose
           #If the 'OTHER_ID' field is non-empty and 'CMTE_ID' and 'TRANSACTION_AMOUNT' fields are empty, ignore the line
           if (splitted[15]!="" or splitted[0]=="" or splitted[14]=="" or (int(splitted[14]))<0):
                continue
           # Filter the required fields and append the resulting data in the list in the form of dictionary
           donarData.append({
               'cmteId': splitted[0],
               'transactionAmount': splitted[14],
               'transactionDate': splitted[13],
               'zipCode': splitted[10][0:5], # Consider only first 5 digits in the Zip Code
            })
           #--------------------------Problem1------------------------
           #Problem1: Find  Running Median by Zip and Receipt Code
           insideLoopOnce=False
           for data in donarData:
                # Ignore data if Zip_Code field is empty or contains less than 5 digits
                if(data['zipCode']=="" or len(data['zipCode'])<5 ):
                    continue
                insideLoopOnce=True
                #pull all the transcation Amounts with the current zip code from the list donarData
                filteredData=[(int)(d['transactionAmount']) for d in donarData if d['zipCode'] == data['zipCode']] 
           if(insideLoopOnce is True):
                #Compute Median
                median=(int)(findMedian(filteredData))
                #Compute total amount
                totalAmount=sum(filteredData)
                #Count the number of transactions
                count=len(filteredData)
                #print(data['cmteId']+"|"+data['zipCode']+"|"+str(median)+"|"+str(count)+"|"+str(totalAmount))
                stringtobeWritten=data['cmteId']+"|"+data['zipCode']+"|"+str(median)+"|"+str(count)+"|"+str(totalAmount)+"\n"
                file.write(stringtobeWritten)  
    file.close()
    # Delete the list to free memory
    del filteredData
    #--------------------------Problem2------------------------
    #Problem2: Find  Median by Time
    currentData=[]# Declare a list to hold the relevant data for the problem

    for data in donarData:
        # Ignore data if date field is empty or invalid
        if ((data['transactionDate'] == "") or (not validateDate(data['transactionDate']))):
            continue
        # Convert the date string into date object using method convertToDateObject
        #No need to validate the date in the convertToDateObject method since all the invalid dates are already filtered
        date=convertToDateObject(data['transactionDate'])
        # store the relevant data in the form of dictionary in the list currentData
        currentData.append({
            'transactionDate': date,
            'cmteId': data['cmteId'],
            'transactionAmount': data['transactionAmount']
        })
    #sort the whole data by ReceiptID
    sortedByReceiptID=sorted(currentData, key=lambda k: (k['cmteId'],k['transactionDate']))
    # pointer to keep track of the current data
    currentPointer=0
    # List for storing the median values of amount with date and other parameters
    medianValueByDate=[]
    #Loop until the end of the sorted list
    while(currentPointer<len(sortedByReceiptID)):
        #pull current CMTE_ID, TRANSACTION_DATE
        currentcmteID=sortedByReceiptID[currentPointer]['cmteId']
        currentDate = sortedByReceiptID[currentPointer]['transactionDate']
        #Declare a list temporayListOfAmount to store the donation amounts of all matching dates and CMTE_ID
        #with the current date and CMTE_ID
        temporaryListOfAmount = []
        # Loop until  CMTE_ID and dtae of the new records are same as the current CMTE_ID and current date.
        while (sortedByReceiptID[currentPointer]['cmteId']==currentcmteID \
        and sortedByReceiptID[currentPointer]['transactionDate']==currentDate ):
            # Append the list temporaryListOfAmount with all new amounts of data satisfying above condition in the
            #  while loop
            temporaryListOfAmount.append(int(sortedByReceiptID[currentPointer]['transactionAmount']))
            #Increment current pointer
            currentPointer = currentPointer + 1
            #Break the inner loop if end of the records is reached
            #If break happens,control will come out of the outer loop as well;
            #  because of the condition in this while loop:'currentPointer<len(sortedByReceiptID)'
            if currentPointer >= len(sortedByReceiptID):
                break
        #print(temporaryListOfAmount)
        # Find the Median of the Amounts
        median = (int)(findMedian(temporaryListOfAmount))
        #Find the total number of transactions in this list
        count=len(temporaryListOfAmount)
        #find the sum of the amounts in this list
        totalAmount=sum(temporaryListOfAmount)
        #Insert the median, count and totalAmount with their corresponding date and CMTE_ID in the list medianValueByDate.
        medianValueByDate.append({
            'cmteId':sortedByReceiptID[currentPointer-1]['cmteId'],
            'transactionDate':(sortedByReceiptID[currentPointer-1]['transactionDate']),
            'median':median,
            'count':count,
            'totalAmount':totalAmount
        })
    #The list is already sorted first by receiptID and then by date cronologically
    #Reformat the date  to the original format
    for i in range(len(medianValueByDate)):
        medianValueByDate[i]['transactionDate']=convertDateObjectToString(medianValueByDate[i]['transactionDate'])
    file  = open(outputFileNameforMedianValuebyTime,"w")
    for ele in medianValueByDate:
        #print(str(ele['cmteId']) + "|" + str(ele['transactionDate']) + "|" + str(ele['median']) + "|" + str(ele['count'])  + "|" + str(ele['totalAmount']) )
        lineString=str(ele['cmteId']) + "|" + str(ele['transactionDate']) + "|" + str(ele['median']) + "|" + str(
            ele['count']) + "|" + str(ele['totalAmount'])+"\n"
        file.write(lineString)
    file.close()
#-----------------------------------------------------------------------------------------------------------------------------------------------------------
#Entry point of Program
#-----------------------------------------------------------------------------------------------------------------------------------------------------------

if __name__ =="__main__":
    start=time.time()
    try:
        inputFileName = sys.argv[1] # Input File Name
        outputFileNameforMedianValuebyZip=sys.argv[2]#Output File Name for Median Value by zip
        outputFileNameforMedianValuebyTime = sys.argv[3]#Output File Name for Median Value by Time
    except IndexError:
        print ("Usage: ./src/find_political_donors.py  ./input/itcont.txt  ./output/medianvals_by_zip.txt  ./output/medianvals_by_date.txt")
        sys.exit(1)
    processData(inputFileName,outputFileNameforMedianValuebyZip,outputFileNameforMedianValuebyTime)
    end=time.time()
    print(end-start)
