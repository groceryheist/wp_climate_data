header = ["page_id_from","page_title_from","page_id_to","page_title_to"]

with open("../data/climate_change_innet.csv","w") as of:
    of.write('\t'.join(["Source","Target"])+'\n')
    for line in open("../data/climate_change_network.csv",'r'):
        fields = line.split('\t')
        if fields[2] == "6266":
            of.write('\t'.join([fields[1],'Climate Change'])+'\n')
        
