import csv
with open(r"./paymo_input/batch_payment.txt", 'rb') as f:
    reader = csv.reader(f, delimiter=',', skipinitialspace = True, quoting=csv.QUOTE_NONE)
    linenumber = 1
    try:
        for row in reader:
            linenumber += 1
    except Exception as e:
        print ("Error line %d: %s" % (linenumber, str(type(e))))
