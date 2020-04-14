#################################################
########### CREATED IN JANUARY - 2020 ###########
###########   Nicolas Porto Campana   ###########
#################################################


import pandas as pd
import sys
import copy
import glob
import os

## Example
## python Generator.py filestructure.xlsx instance_folder destination_folder GraphType #Instances

class Generator:
    """
    # Generator Class

    Generates the instances of ALBHW problem

    """
    h1Cost = 100
    h1Prop = 0.2
    h2Prop = 0.3
    h3Prop = 0.5 
    w1 = (1.10,1.20)
    w2 = (0.70,0.85)
    qntLavoratori = 3
    groups = None
    instances = {}
    pathInstances = ""
    pathDestination = ""
    pathDetails = ""
    dict = {"bimodal":"BM", "peak at the bottom":"PB", "peak in the middle": "PM", \
        "extremely tricky":"ET", "very tricky": "VT", "less tricky":"LT", "tricky": "TR", "open (not known yet)": "OP"}
    def __init__(self, pathDetails : str, pathInstances : str, pathDestination : str):
        """
        # Input: 
        pathDetails => Details file from SALB

        pathInstances => Where are located the SALB instances
        
        pathDestination => Where will be created the instances of ALBHW
        
        """
        self.pathDetails = pathDetails
        self.pathInstances = pathInstances
        self.pathDestination = pathDestination
        self.create_folders()

    def create_folders(self):
        os.makedirs(self.pathDestination + "Large/Instance1.0-1.0")
        for r1 in self.w1:
            for r2 in self.w2:
                os.makedirs(self.pathDestination + "Large/Instance" + str(r1) + "-" + str(r2))

    def select_data(self, type: str):
        """
        # Parameters
        
        1 => Path details file

        2 => Type of ALB: BN , CH, MIXED, ALL 

        """
        try:
            details = pd.read_excel(self.pathDetails, skiprows=[0])
        except Exception:
            print("Not able to open the file: {0}".format(path))
        selected = details.loc[(details["<Graph structures>"] == type) & (details["<Trickiness category>"] != "open (not known yet)")].copy()
        # Order by Time Distribuition and Desired OS
        self.groups = selected.groupby(["<Times distribution>", "<Desired OS>"])

    def choosing_instances(self, n : int):
        for name, group in self.groups:
            # Create the keys 
            for i in group.iloc[:,5:6].values:
                if((name[0], name[1], i[0]) not in self.instances):
                    self.instances[(name[0], name[1], i[0])] = []
            if(len(group) < n):
                qnt = len(group)
            else:
                qnt = n
            inst = group.sample(qnt)
            for i in inst.values:
                self.instances[(name[0], name[1], i[5])].append(i[1])

    def creating_instancesSALB(self):
        for i,k in self.instances.items():
            for ins in k:
                with open(self.pathInstances + ins + ".alb", "r") as instance:
                    instanceRaw = [i.replace(","," ") for i in instance.readlines()[:-1]]
                    instanceRaw.append("<type workers>\n")
                    instanceRaw.append(str(1)+"\n")
                    nTasks = int(instanceRaw[1].strip('\n'))
                    instanceRaw.append("<task types>\n")
                    tTypes = []
                    cycT = int(instanceRaw[4])
                    for aux in range(0, nTasks):
                        instanceRaw.append("1\n")
                        tTypes.append(1)        
                    instanceRaw.append("<task times>\n")
                    tTimes = [int(times.strip('\n').split(" ")[1]) for times in instanceRaw[11:nTasks+11]]
                    for aux in range(0, nTasks):
                        instanceRaw.append(str(tTimes[aux]) + "\n")
                    instanceRaw.append("<worker costs>\n")
                    instanceRaw.append(str(self.h1Cost) + "\n")
                    instanceRaw.append("<end>")
                    nameFile = str(ins.split("_")[2])+ "_"+ str(1.0) + "_" + str(1.0) + "_" + str(i[1])   + "_" + self.dict[i[0]]+ "_" + self.dict[i[2]] + ".albhw"
                    namePath = self.pathDestination + "Large/Instance" + str(1.0) + "-" + str(1.0) + "/L"                  
                    with open(namePath+nameFile, "w") as newInstance:
                        newInstance.writelines(instanceRaw)
                    
                            
    def creating_instancesALBHW(self):
        for rule1 in range(len(self.w1)):
            for rule2 in range(len(self.w2)):
                for i,k in self.instances.items():
                    for ins in k:
                        with open(self.pathInstances + ins + ".alb", "r") as instance:
                            instanceRaw = [i.replace(","," ") for i in instance.readlines()[:-1]]
                            instanceRaw.append("<type workers>\n")
                            instanceRaw.append(str(self.qntLavoratori)+"\n")
                            nTasks = int(instanceRaw[1].strip('\n'))
                            instanceRaw.append("<task types>\n")
                            tTypes = []
                            cycT = int(instanceRaw[4])
                            for aux in range(0, int(nTasks*self.h1Prop)):
                                instanceRaw.append("1\n")
                                tTypes.append(1)        
                            for aux in range(0, int(nTasks*self.h2Prop)):
                                instanceRaw.append("2\n")
                                tTypes.append(2)        
                            for aux in range(0, int(nTasks*self.h3Prop)):
                                instanceRaw.append("3\n")
                                tTypes.append(3)        
                            instanceRaw.append("<task times>\n")
                            tTimes = [int(times.strip('\n').split(" ")[1]) for times in instanceRaw[11:nTasks+11]]
                            for aux in range(0, nTasks):
                                worker2 = round(tTimes[aux] * self.w1[rule1]) if 2 <= tTypes[aux] else "INF"
                                if(worker2 != "INF" and worker2 <= cycT):
                                    worker3 = round(worker2 * self.w1[rule1]) if 3 <= tTypes[aux] else "INF"
                                else: 
                                    worker3 = "INF"
                                instanceRaw.append(str(tTimes[aux]) + " " + str(worker2) + " " + str(worker3) + "\n")
                            instanceRaw.append("<worker costs>\n")
                            instanceRaw.append(str(self.h1Cost) + "\n")
                            calc = self.h1Cost * self.w2[rule2]
                            calc2 = calc * self.w2[rule2]
                            instanceRaw.append(str(round(calc)) + "\n")
                            instanceRaw.append(str(round(calc2)) + "\n")
                            instanceRaw.append("<end>")
                            namePath = self.pathDestination + "Large/Instance" + str( self.w1[rule1]) + "-" + str( self.w2[rule2])
                            nameInstance = "/L" + str(ins.split("_")[2])+ "_"+ str(self.w1[rule1]) + "_" + str(self.w2[rule2]) + "_" + str(i[1])   + "_" + self.dict[i[0]]+ "_" + self.dict[i[2]] + ".albhw"
                            with open(namePath + nameInstance, "w") as newInstance:
                                newInstance.writelines(instanceRaw)
        
    


genera = Generator(sys.argv[1], sys.argv[2], sys.argv[3])
genera.select_data(sys.argv[4])
genera.choosing_instances(int(sys.argv[5]))
genera.creating_instancesSALB()
genera.creating_instancesALBHW()

