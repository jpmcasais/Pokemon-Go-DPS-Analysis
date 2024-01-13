#Hello World.
#The project's objective is to efficiently process raw data from diverse sources using Python, ultimately generating a consolidated table that encompasses all essential information for subsequent analysis.  
#The focus of the analysis revolves around Pokémon Go, specifically delving into the Damage Per Second (DPS) of each Pokémon. This entails examining the DPS for each attack and level of the Pokémon in order to gain comprehensive insights.

##Version 1.0

#-Calculated DPS simple (DPSs) based with a perfect pokemon


##Backlog

#-Add the full analysis on each Pokemon Level
#-Add the full analysis on each Pokemon IVs
#-Add the Weatherboost
#-Add the Effectiveness
#-Add the Mega Boost
#-Add the Friendship Boost
#-Create Visuals in Python
#-Create a Report


#-------------

#The initial step involved gathering all crucial data necessary for constructing the Master Table.

##Base Stats - https://bulbapedia.bulbagarden.net/wiki/List_of_Pokémon_by_base_stats_(GO)
##Pokemon_type - https://pokemondb.net/pokedex/all
##Fast_Move - https://bulbapedia.bulbagarden.net/wiki/List_of_moves_(GO)
##Charged_Move - https://bulbapedia.bulbagarden.net/wiki/List_of_moves_(GO)
##CPM - https://gamepress.gg/pokemongo/cp-multiplier
##Combo Moves - https://gamepress.gg/pokemongo/comprehensive-dps-spreadsheet

#Once all the data was located, it was exported to an Excel file that serves as the repository for consolidating and organizing all information, with the exception of the "Combo Moves.csv" file.

#The subsequent phase involves analyzing how the data is interpreted and making adjustments as necessary to ensure accurate processing.


#Import Libraries
import pandas as pd
import numpy as np


#Import the List of Pokemons in order to extract the base stats from the excel sheet
Base_Stats = pd.read_excel(r"C:\Users\joao\Desktop\POG\Pokemon_Data.xlsx", sheet_name="Base_Stats")
Base_Stats.head(10)
Base_Stats.info()

#Analyzing the first 10 rows of the dataset and the content of each column. Certain transformations are required to rectify the data.

#1. Remove unnecessary columns.
Base_Stats = Base_Stats.drop(columns={"Unnamed: 1", "Product", "Max CP.1", "Max CP.2", "Max CP"})

#2. Correct column names.
Base_Stats = Base_Stats.rename(columns={"#": "ID", "Pokémon": "Pokemon"})
Base_Stats

#3. Exclude the first row.
Base_Stats = Base_Stats.drop([0])
Base_Stats

#4. Remove rows with missing Pokémon names (NAN).
Base_Stats = Base_Stats[Base_Stats['Pokemon'].notnull()]
Base_Stats

#5. Reset Index.
Base_Stats.reset_index(drop=True, inplace=True)
Base_Stats
#We have duplicate Pokémon names due to combined cells in the extracted data sheet that identify both the Pokémon and the Special Evolution. The objective is to merge these two cells into one. For example, at Index 3, the data from Index 4 is present, but Index 4 values are empty. To populate these values, an automatic combination of these two rows into the full table is necessary.


#1. Add a new index. Creating a fresh index enables the tracking of the data order and identification of each unique row.
Base_Stats['Index'] = Base_Stats.index + 1

#2. Create a new conditional index. For the rows where the cells that were supposed to be combined have an empty "ID" cell, this implies that we can utilize this empty cell to attribute a value to the new index column. Consequently, the output index for this row will be NAN. Thus, a new column named "New Index" can be created with the condition that if a value exists in "ID," return the index; otherwise, return NAN.
Base_Stats['New index'] = Base_Stats['Index'].where(Base_Stats['ID'].notnull(), None)

#3. Fill down the "New Index" column. The objective is to establish a unique index that facilitates the identification of row groups for subsequent combination in the following steps.
Base_Stats['New index'].fillna(method='ffill', inplace=True) 

#4. Group by New index column and concatenate text in Pokémon column
Base_Stats_BS = Base_Stats.groupby('New index')['Pokemon'].apply(lambda x: ' '.join(x)).reset_index()
Base_Stats_BS

#5. Left join the Base_Stats with Base_Stats_BS
Base_Stats = pd.merge(Base_Stats, Base_Stats_BS, on='New index', how='left')

#6. Drop the Pokemons that have no ID in the ID Column
Base_Stats = Base_Stats[Base_Stats['ID'].notnull()]
Base_Stats

#7. Drop the Columns used for the Transformation
Base_Stats = Base_Stats.drop(columns=['New index', 'Pokemon_x', "Index"])
Base_Stats

#8. Rename Pokemon column name
Base_Stats = Base_Stats.rename(columns={"Pokemon_y": "Pokemon"})
Base_Stats
#As of now, all necessary changes have been implemented. Later on, we will need to adjust the Pokémon names to align with the other tables.



#Import the List of Pokemons in order to extract the Type 1 and Type 2 from the excel sheet
Pokemon_type = pd.read_excel(r"C:\Users\joao\Desktop\POG\Pokemon_Data.xlsx", sheet_name="Pokemon_type")
Pokemon_type.head(10)
Pokemon_type.info()

#Analyzing the first 10 rows of the dataset and the content of each column. Certain transformations are required to rectify the data.

#1. Remove unnecessary columns.
Pokemon_type = Pokemon_type[["#","Type"]]

#2. Fill down the "#" Column.
Pokemon_type['#'].fillna(method='ffill', inplace=True)
Pokemon_type

#3. Remove the Pokémon ID strings from the "#" column. Remove the last 4 strings
Pokemon_type['#'] = Pokemon_type["#"].apply(lambda x: x[:-4])

#4. Rename Pokemon column name
Pokemon_type = Pokemon_type.rename(columns={"#": "Pokemon"})

#5. Split the "Type" column into two columns for each unique Pokémon.
#5.1 Merge the rows with the same Pokémon name, separated by a comma delimiter.
Pokemon_type = Pokemon_type.groupby('Pokemon')['Type'].apply(lambda x: ','.join(x.dropna()) if x.notna().any() else np.nan).reset_index(name='Type')
Pokemon_type

#5.2 Separate the "Type" column by a comma delimiter.
Pokemon_type[['Type1', 'Type2']] = Pokemon_type['Type'].str.split(',', expand=True)
Pokemon_type

#5.3 Remove "Type" column.
Pokemon_type = Pokemon_type.drop(columns=["Type"])



#Perform a left join between the "Base_Stats" table and the "Pokemon_type" table. Certain transformations are required to rectify the data.
merged_data_View = pd.merge(Base_Stats, Pokemon_type, on='Pokemon', how='left')
merged_data_View.head(10)
merged_data_View.info()
#The count of Pokémon entries should be equal for both the overall Pokémon count and the Type1 count. The primary goal is to make adjustments to the tables to ensure this alignment.


#Now, we need to examine the list of Pokémon from the "Pokemon Type" table that didn't match with the list from the "Base_Stats" table. This will provide us with a record of unmatched Pokémon between the tables.
merged_data_View = merged_data_View[pd.isnull(merged_data_View["Type1"])]
merged_data_View


#Rectify Pokémon names in the "Base_Stats" table.

#1. Create a duplicate of the "Base_Stats" table for backup or reference purposes.
TBase_stats = Base_Stats.copy()

#2. After previewing the initial output of "merged_data_View," it is evident that Pokémon forms have formatting issues, where the normal name is presented first and followed by the form name. The primary objective is to correct the formatting of these names.
#2.1 Generate a new column with labels such as "Mega," "Alolan," "Galarian," and "Hisuian" to identify Pokémon forms in the dataset.
TBase_stats["is_especial"] = np.where(TBase_stats['Pokemon'].str.contains(' Mega ')==True, "Mega",np.where(TBase_stats['Pokemon'].str.contains(' Alolan ')==True, "Alolan",np.where(TBase_stats['Pokemon'].str.contains(' Galarian ')==True, "Galarian",np.where(TBase_stats['Pokemon'].str.contains(' Hisuian ')==True, "Hisuian",""))))

#2.2 Create a duplicate column for Pokémon names.
TBase_stats["name dup"] =  TBase_stats.loc[:,"Pokemon"]
TBase_stats

#Correct the names of Pokémon with "Mega" forms in the dataset.
#2.3.1 Remove all text before "Mega" and include it in a new column labeled "name dup".
TBase_stats["name dup"] = TBase_stats["name dup"].str.split(" Mega ").str[1]
TBase_stats

#2.3.2 Replace empty values in the "Name dup" column with the corresponding values from the "Pokemon" column.
TBase_stats['name dup'] = TBase_stats['name dup'].fillna(TBase_stats.pop('Pokemon'))
TBase_stats

#2.3.3 Combine the columns "is_especial" and "Name dup."
TBase_stats["name dup"] = TBase_stats["is_especial"].astype(str) + " " + TBase_stats["name dup"]
TBase_stats

#2.3.4 Rename Pokemon column name
TBase_stats = TBase_stats.rename(columns={"name dup": "Pokemon"})
TBase_stats

#Correct the names of Pokémon with "Alolan Form" forms in the dataset.
#2.4 Replace the term "Alolan Form" with an empty string.
TBase_stats["Pokemon"] = TBase_stats["Pokemon"].str.replace("Alolan Form","")

#Correct the names of Pokémon with "Galarian Form" forms in the dataset.
#2.5 Replace the term "Galarian Form" with an empty string.
TBase_stats["Pokemon"] = TBase_stats["Pokemon"].str.replace("Galarian Form","")

#2.6 Correct the names of Pokémon with "Hisuian Form" and "Galarian Form" in the dataset.
TBase_stats["Pokemon"] = TBase_stats["Pokemon"].str.replace("Hisuian From","Hisuian Form")
TBase_stats["Pokemon"] = TBase_stats["Pokemon"].str.replace("Galarian From","Galarian Form")

#2.7 Correct manually entered Pokémon names.
TBase_stats["Pokemon"] = TBase_stats["Pokemon"].str.replace("Kyogre Primal Kyogre","Primal Kyogre")
TBase_stats["Pokemon"] = TBase_stats["Pokemon"].str.replace("Groudon Primal Groudon","Primal Groudon")
TBase_stats["Pokemon"] = TBase_stats["Pokemon"].str.replace("Mewtwo Armored Mewtwo","Armored Mewtwo")
TBase_stats["Pokemon"] = TBase_stats["Pokemon"].str.replace("Galarian Darmanitan , Standard Mode","Darmanitan Standard Mode")
TBase_stats["Pokemon"] = TBase_stats["Pokemon"].str.replace("Galarian Darmanitan , Zen Mode","Darmanitan Zen Mode")
TBase_stats["Pokemon"] = TBase_stats["Pokemon"].str.replace("Galarian Yamask Galarian Form","Galarian Yamask")
TBase_stats["Pokemon"] = TBase_stats["Pokemon"].str.replace("Oricorio", "Oricorio Baile Style")

#Correct the names of Pokémon with "Hisuian Form" forms in the dataset.
#2.8 Replace the term "Hisuian Form" with an empty string.
TBase_stats["Pokemon"] = TBase_stats["Pokemon"].str.replace("Hisuian Form","")

#3 Remove whitespaces from the Pokémon names.
TBase_stats["Pokemon"] = TBase_stats["Pokemon"].str.strip()



#Rectify Pokémon names in the "Pokemon_type" table.

#1. Create a duplicate of the "Pokemon_type" table for backup or reference purposes.
TPokemon_type = Pokemon_type.copy()

#2. Remove parentheses "(" and ")" from the Pokémon names.
TPokemon_type["Pokemon"] = TPokemon_type["Pokemon"].str.replace("(","", regex=False)
TPokemon_type["Pokemon"] = TPokemon_type["Pokemon"].str.replace(")","", regex=False)


#3. Create a duplicate column for Pokémon names.
TPokemon_type["name dup"] =  TPokemon_type.loc[:,"Pokemon"]

#Correct the names of Pokémon with "Hisuian Form" forms in the dataset.
#4.1 Generate a new column with labels such as "Hisuian" to identify Pokémon forms in the dataset.
TPokemon_type["is_especial"] = np.where(TPokemon_type['Pokemon'].str.contains(' Hisuian ')==True, "Hisuian","")

#4.2 Remove all text before "Hisuian" and include it in a new column labeled "name dup".
TPokemon_type["name dup"] = TPokemon_type["name dup"].str.split(" Hisuian ").str[1]

#4.3 Replace empty values in the "Name dup" column with the corresponding values from the "Pokemon" column.
TPokemon_type['name dup'] = TPokemon_type['name dup'].fillna(TPokemon_type.pop('Pokemon'))

#4.4 Combine the columns "is_especial" and "Name dup.
TPokemon_type["name dup"] = TPokemon_type["is_especial"].astype(str) + " " + TPokemon_type["name dup"]

#4.5 Rename Pokemon column name
TPokemon_type = TPokemon_type.rename(columns={"name dup": "Pokemon"})
TPokemon_type

#5. Correct manually entered Pokémon names.
TPokemon_type["Pokemon"] = TPokemon_type["Pokemon"].str.replace("Ho-oh","Ho-Oh")
TPokemon_type["Pokemon"] = TPokemon_type["Pokemon"].str.replace("Burmy Sandy Cloak","Burmy")
TPokemon_type["Pokemon"] = TPokemon_type["Pokemon"].str.replace("Meowstic Male","Meowstic")
TPokemon_type["Pokemon"] = TPokemon_type["Pokemon"].str.replace("Wooper Paldean Wooper","Wooper Paldean Form")

#Create a list of Pokémon that do not exist in the table.
#6.1. Add rows Armored Mewtwo, Cherrim Overcast Form, Cherrim Sunshine Form, Shellos West Sea, Shellos East Sea, Gastrodon West Sea, Gastrodon East Sea
New_TPokemon_type = {"Type1":["Psychic", "Grass","Grass","Water","Water","Water","Water"], "Type2":["","","","","","Ground","Ground"], "Pokemon":["Armored Mewtwo","Cherrim Overcast Form","Cherrim Sunshine Form","Shellos West Sea","Shellos East Sea","Gastrodon West Sea","Gastrodon East Sea"]}
New_TPokemon_type =pd.DataFrame(New_TPokemon_type)

#6.2. Merge the "TPokemon_type" and "New_TPokemon_type" tables.
TPokemon_type = pd.concat([TPokemon_type, New_TPokemon_type], ignore_index=True)

#7. Remove whitespaces from the Pokémon names.
TPokemon_type["Pokemon"] = TPokemon_type["Pokemon"].str.strip()


#Perform a left join between the "Base_Stats" table and the "Pokemon_type" table to identify the remaining unmatched Pokémon.
merged_data1 = pd.merge(TBase_stats, TPokemon_type, on='Pokemon', how='left')
Pokemon_BT = merged_data1.copy()
merged_data1.info()

#List of unmatched Pokémon between the tables. If "Merged Data1" returns any values, it is necessary to add the missing Pokémon to the table until the table is empty.
merged_data1 = merged_data1[pd.isnull(merged_data1["Type1"])]
merged_data1

#Remove unnecessary columns.
Pokemon_BT = Pokemon_BT.drop(columns=["is_especial_x","is_especial_y"])
print(Pokemon_BT)




#Import the List of Fast Moves in order to extract the details from the excel sheet
Fast_Move = pd.read_excel(r"C:\Users\joao\Desktop\POG\Pokemon_Data.xlsx", sheet_name="Fast_Move")
Fast_Move.info()
Fast_Move.head(10)

#1. Correct column names.
Fast_Move = Fast_Move.rename(columns={"Unnamed: 1": "Fast Move", "Unnamed: 2": "Type", "Duration (s)":"Duration"})

#2. Remove unnecessary columns.
Fast_Move = Fast_Move.drop(columns={"Unnamed: 0", "Damage window (s)","Unnamed: 7", "Power.1", "Energy boost.1","Turns"})
Fast_Move = Fast_Move.dropna()
#As of now, all necessary changes have been implemented. Later on, we will need to adjust the Fast Moves names to align with the other tables.


#Import the List of Charged Moves in order to extract the details from the excel sheet
Charged_Move = pd.read_excel(r"C:\Users\joao\Desktop\POG\Pokemon_Data.xlsx", sheet_name="Charged_Move")
Charged_Move.info()
Charged_Move.head(10)

#1. Correct column names.
Charged_Move = Charged_Move.rename(columns={"Unnamed: 1": "Charged Move", "Unnamed: 2": "Type", "Duration (s)":"Duration"})

#2. Remove unnecessary columns.
Charged_Move = Charged_Move.drop(columns={"Unnamed: 0", "Unnamed: 5","Unnamed: 6", "Unnamed: 9","Power.1","Energy cost.1","Unnamed: 12", "Stat modifiers","Unnamed: 14"})
Charged_Move = Charged_Move.dropna()
#As of now, all necessary changes have been implemented. Later on, we will need to adjust the Charged Moves names to align with the other tables.


#Import the list of move combinations per Pokémon from the CSV file.
Moves=pd.read_csv(r"C:\Users\joao\Desktop\POG\comprehensive_dps.csv")
Moves.info()
Moves.head(10)

#1. Change column name.
Moves = Moves.rename(columns={"Pokemon":"Pokemon Go"})

#2. Correct manually entered Pokémon names. Later in the analysis, we will focus on identifying Pokémon with incorrect names.
Moves["Pokemon Go"] = Moves["Pokemon Go"].str.replace("Shdow Alolan Vulpix","Shadow Alolan Vulpix")

#3. Create a column that indicates whether the Pokémon is a shadow or not.
Moves["is_shadow"] = np.where(Moves['Pokemon Go'].str.contains('Shadow ')!=False, True,False)

#4. Create a column that indicates whether the Pokémon is a Mega, Primal or not.
Moves["is_mega"] = np.where(Moves['Pokemon Go'].str.contains('Mega ')==True, True,np.where(Moves['Pokemon Go'].str.contains('Primal ')==True, True,False))

#5. Remove the "shadow" designation from the Pokémon names in the column to facilitate the merging of datasets.
Moves["Pokemon"] = Moves.loc[:,"Pokemon Go"]
Moves["Pokemon"] = Moves["Pokemon"].str.replace("Shadow ","")

#6. Remove whitespaces from the Pokémon names.
Moves["Pokemon"] = Moves["Pokemon"].str.strip()



#Perform a left join between the "Moves" table and the "Pokemon_BT" table to identify the remaining unmatched Pokémon.
Merge = pd.merge(Moves, Pokemon_BT, on='Pokemon', how='left')
Merge.info()
Merge.head(10)

#List of unmatched Pokémon between the tables.
Merge = Merge[pd.isnull(Merge["ID"])]
Merge



#Rectify Pokémon names in the "Moves" table.
#1. Create a duplicate of the "Moves" table for backup or reference purposes.
TMoves = Moves.copy()

#2. Remove unnecessary columns.
TMoves = TMoves.drop(columns={"DPS", "TDO","ER", "CP"})

#3. Remove parentheses "(", ")" and " - " from the Pokémon names.
TMoves["Pokemon"] = TMoves["Pokemon"].str.replace("(","", regex=False)
TMoves["Pokemon"] = TMoves["Pokemon"].str.replace(")","", regex=False)
TMoves["Pokemon"] = TMoves["Pokemon"].str.replace(" - "," ", regex=False)

#Correct manually entered Pokémon names.
#4.1 Fix Genesect Name
TMoves['is_genesect'] = TMoves['Pokemon'].str.contains('Genesect')
TMoves.loc[TMoves['is_genesect'], 'Pokemon'] = 'Genesect'

#4.2 Fix Pikachu Name
TMoves['is_pikachu'] = TMoves['Pokemon'].str.contains('Pikachu')
TMoves.loc[TMoves['is_pikachu'], 'Pokemon'] = 'Pikachu'

#4.3 Manual Replace
TMoves["Pokemon"] = TMoves["Pokemon"].str.replace("Thundurus  Incarnate Forme","Thundurus Incarnate Forme")
TMoves["Pokemon"] = TMoves["Pokemon"].str.replace("Meowstic Female","Meowstic")
TMoves["Pokemon"] = TMoves["Pokemon"].str.replace("Meowstic Male","Meowstic")

#5. Remove unnecessary columns.
TMoves = TMoves.drop(columns={"is_genesect", "is_pikachu"})



#Rectify Pokémon names in the "Pokemon_BT" table.
#1. Create a duplicate of the "Pokemon_BT" table for backup or reference purposes.
TPokemon_BT = Pokemon_BT.copy()


#Correct manually entered Pokémon names.
#2.1 Fix rotom name
TPokemon_BT["Pokemon"] = TPokemon_BT["Pokemon"].str.split(pat=" Rotom",n=1).str[0]

#2.2 Manual Replace
TPokemon_BT["Pokemon"] = TPokemon_BT["Pokemon"].str.replace("Kyurem Black Kyurem","Black Kyurem")
TPokemon_BT["Pokemon"] = TPokemon_BT["Pokemon"].str.replace("Hoopa Hoopa Unbound","Hoopa Unbound")
TPokemon_BT["Pokemon"] = TPokemon_BT["Pokemon"].str.replace("Hoopa Hoopa Confined","Hoopa Confined")
TPokemon_BT["Pokemon"] = TPokemon_BT["Pokemon"].str.replace("Keldeo Resolute Form","Keldeo")
TPokemon_BT["Pokemon"] = TPokemon_BT["Pokemon"].str.replace("Darmanitan Standard Mode","Darmanitan")
TPokemon_BT["Pokemon"] = TPokemon_BT["Pokemon"].str.replace("Darmanitan Zen Mode","Galarian Darmanitan")
TPokemon_BT["Pokemon"] = TPokemon_BT["Pokemon"].str.replace("Basculin Red-Striped Form","Basculin")
TPokemon_BT["Pokemon"] = TPokemon_BT["Pokemon"].str.replace("Aegislash Shield Forme","Aegislash Shield")
TPokemon_BT["Pokemon"] = TPokemon_BT["Pokemon"].str.replace("Castform Sunny Form","Castform Sunny")
TPokemon_BT["Pokemon"] = TPokemon_BT["Pokemon"].str.replace("Castform Rainy Form","Castform Rainy")
TPokemon_BT["Pokemon"] = TPokemon_BT["Pokemon"].str.replace("Castform Snowy Form","Castform Snowy")

#2.3 Create a list of Pokémon that do not exist in the table.
New_TPokemon_BT = {"ID":[924,844,741,741,741,79,833], "HP":[137,176,181,181,181,207,137], "Attack":[98,202,196,196,196,109,114], "Defense":[90,207,145,145,145,98,85], "Pokemon":["Tandemaus","Sandaconda","Oricorio Sensu Style","Oricorio Pom-Pom Style", "Oricorio Pa'u Style", "Galarian Slowpoke", "Chewtle"], "Type1":["Normal", "Ground", "Flying", "Eletric","Flying", "Psychic","Water"], "Type2":["", "", "Ghost", "Flying", "Psychic","Water",""]}
New_TPokemon_BT =pd.DataFrame(New_TPokemon_BT)

#2.4 Merge the "TPokemon_type" and "New_TPokemon_type" tables.
TPokemon_BT = pd.concat([TPokemon_BT, New_TPokemon_BT], ignore_index=True)
TPokemon_BT = TPokemon_BT.drop_duplicates(subset=["Pokemon"])

#Perform a left join between the "TMoves" table and the "TPokemon_BT" table to identify the remaining unmatched Pokémon.
Pokemon_BTM = pd.merge(TMoves, TPokemon_BT, on='Pokemon', how='left')

#List of unmatched Pokémon between the tables. If "Merge1" returns any values, it is necessary to add the missing Pokémon to the table until the table is empty.
Merge1 = Pokemon_BTM[pd.isnull(Pokemon_BTM["ID"])]
Merge1.groupby(['Pokemon'])['Pokemon'].count()
Merge1


#Rectify Fast Moves names in the "Pokemon_BTM" table.
#1. Remove whitespaces from the Pokémon names.
Pokemon_BTM["Fast Move"] = Pokemon_BTM["Fast Move"].str.strip()

#Perform a left join between the "Pokemon_BTM" table and the "Fast_Move" table to identify the remaining unmatched Fast Moves.
Fast_Pokemon_BTMview = pd.merge(Pokemon_BTM, Fast_Move, on='Fast Move', how='left')
Fast_Pokemon_BTMview.info()
Fast_Pokemon_BTMview.head(10)


#List of unmatched Pokémon between the tables. If "Fast_Pokemon_BTMview" returns any values, it is necessary to add the missing Pokémon to the table until the table is empty.
Fast_Pokemon_BTMview = Fast_Pokemon_BTMview[pd.isnull(Fast_Pokemon_BTMview["Type"])]
Fast_Pokemon_BTMview.groupby(['Fast Move'])['Fast Move'].count()


#1. Create a duplicate of the "Fast_Move" table for backup or reference purposes.
TFast_Move = Fast_Move.copy()

#Correct manually entered Pokémon names.
#2.1 Fix Mud slap move
TFast_Move["Fast Move"] = TFast_Move["Fast Move"].str.replace("Mud-Slap","Mud Slap")


#3 Create a list of Pokémon that do not exist in the table.
#3.1 add Hidden Power moves
Hidden_Power = [
    ("Hidden Power (Electric)", 15, 15, 1.5),
    ("Hidden Power (Fire)", None, None, None),
    ("Hidden Power (Fighting)", None, None, None),
    ("Hidden Power (Water)", None, None, None),
    ("Hidden Power (Flying)", None, None, None),
    ("Hidden Power (Grass)", None, None, None),
    ("Hidden Power (Poison)", None, None, None),
    ("Hidden Power (Ground)", None, None, None),
    ("Hidden Power (Psychic)", None, None, None),
    ("Hidden Power (Rock)", None, None, None),
    ("Hidden Power (Ice)", None, None, None),
    ("Hidden Power (Dragon)", None, None, None),
    ("Hidden Power (Ghost)", None, None, None),
    ("Hidden Power (Dark)", None, None, None),
    ("Hidden Power (Steel)", None, None, None),
    ("Hidden Power (Bug)", None, None, None),
    ("Hidden Power", None, None, None),
]

Hidden_Power_columns = ("Fast Move", "Power", "Energy boost", "Duration")
Hidden_Power = pd.DataFrame(Hidden_Power, columns=Hidden_Power_columns)
Hidden_Power.fillna(method='ffill', inplace=True)
Hidden_Power['Type'] = Hidden_Power['Fast Move'].str.extract(r'\((.*?)\)')
Hidden_Power['Type'].fillna("Normal", inplace=True)

#3.2 Add the rest of the Fast Moves
MissingFast_Moves = {"Fast Move":["Water Shuriken","Geomancy","Leafage"], "Type":["Water","Fairy","Grass"],"Power":[10,20,9], "Energy boost":[15,14,6], "Duration":[1.1,1.5,0.87]}
MissingFast_Moves =pd.DataFrame(MissingFast_Moves)
TFast_Move = pd.concat([TFast_Move, Hidden_Power, MissingFast_Moves], ignore_index=True)

#4. Change columns names.
TFast_Move = TFast_Move.rename(columns={"Power": "FPower","Type":"FType", "Energy boost":"FEnergy boost","Duration":"FDuration"})



#Perform a left join between the "Pokemon_BTM" table and the "TFast_Move" table to identify the remaining unmatched Fast Moves.
Fast_Pokemon_BTMview = pd.merge(Pokemon_BTM, TFast_Move, on='Fast Move', how='left')

##List of unmatched Pokémon between the tables. If "Fast_Pokemon_BTMview" returns any values, it is necessary to add the missing Pokémon to the table until the table is empty.
Fast_Pokemon_BTMview = Fast_Pokemon_BTMview[pd.isnull(Fast_Pokemon_BTMview["FType"])]
Fast_Pokemon_BTMview.groupby(['Fast Move'])['Fast Move'].count()



#Rectify Charged Moves names in the "Pokemon_BTM" table.
#1. Remove whitespaces from the Pokémon names.
Pokemon_BTM["Charged Move"] = Pokemon_BTM["Charged Move"].str.strip()

#Perform a left join between the "Pokemon_BTM" table and the "Charged_Move" table to identify the remaining unmatched Charged Moves.
Charged_Pokemon_BTMview = pd.merge(Pokemon_BTM, Charged_Move, on='Charged Move', how='left')
Charged_Pokemon_BTMview.info()
Charged_Pokemon_BTMview.head(10)


#List of unmatched Pokémon between the tables. If "Charged_Pokemon_BTMview" returns any values, it is necessary to add the missing Pokémon to the table until the table is empty.
Charged_Pokemon_BTMview = Charged_Pokemon_BTMview[pd.isnull(Charged_Pokemon_BTMview["Type"])]
Charged_Pokemon_BTMview.groupby(['Charged Move'])['Charged Move'].count()

#1. Create a duplicate of the "Charged_Move" table for backup or reference purposes.
TCharged_Move = Charged_Move.copy()

#Correct manually entered Pokémon names.
#2.1 Fix Tri-Attack move
TCharged_Move["Charged Move"] = TCharged_Move["Charged Move"].str.replace("Tri Attack","Tri-Attack")

#2.2 Create a list of Pokémon that do not exist in the table.
#2.2.1 add techno blast and Weather Ball moves
Techno_Blast_and_Weather_Ball = [
    ("Techno Blast (Burn)", 120, 100, 2, "Fire"),
    ("Techno Blast (Chill)", None, None, None, "Ice"),
    ("Techno Blast (Douse)", None, None, None, "Water"),
    ("Techno Blast (Shock)", None, None, None, "Electric"),
    ("Techno Blast (Normal)", None, None, None, "Normal"),
    ("Weather Ball Ice", 55, 33, 1.6, "Ice"),
    ("Weather Ball Rock", None, None, None, "Rock"),
    ("Weather Ball Fire", None, None, None, "Fire"),
    ("Weather Ball Water", None, None, None, "Water"),
]

Techno_Blast_and_Weather_Ball_columns = ("Charged Move", "Power", "Energy cost", "Duration",  "Type")

Techno_Blast_and_Weather_Ball = pd.DataFrame(Techno_Blast_and_Weather_Ball, columns=Techno_Blast_and_Weather_Ball_columns)

Techno_Blast_and_Weather_Ball.fillna(method='ffill', inplace=True)

#2.2.2 Add the rest of the Charged Moves
MissingCharged_Moves = [
    ("Dragon Ascent", 140, 50, 3.5, "Flying", 3.2),
    ("Fusion Flare", 140, 100, 2, "Fire",1.5),
    ("Fusion Bolt", 140, 100, 2, "Electric",1.25),
    ("Oblivion Wing", 85, 50, 2, "Flying",1.5),
    ("Breaking Swipe", 35, 33, 0.8, "Dragon",0.27),
    ("Glaciate", 160, 100, 2.5, "Ice",1.5),
    ("Magma Storm", 75, 33, 2.5, "Fire",1.3),
    ("Double Iron Bash", 77, 33, 2, "Steal",1.3),
    ("Mystical Fire", 60, 33, 2, "Fire",1.3),
    ("Psychic Fangs", 30, 33, 1.2, "Psychic",0.4),
    ("Boomburst", 140, 100, 2.3, "Normal",1),
    ("Scorching Sands", 95, 50, 3.2, "Ground",1.6),
    ("Trailblaze", 65, 50, 2, "Grass",1.2),
    ("Triple Axel", 60, 33, 2, "Ice",1.5),

]

MissingCharged_Moves_columns = ("Charged Move", "Power", "Energy cost", "Duration","Type", "Damage window (s)")

MissingCharged_Moves = pd.DataFrame(MissingCharged_Moves, columns=MissingCharged_Moves_columns)
MissingCharged_Moves

#2.3 Merge the "TCharged_Move", "MissingCharged_Moves" and "Techno_Blast_and_Weather_Ball" tables.
TCharged_Move = pd.concat([TCharged_Move, MissingCharged_Moves, Techno_Blast_and_Weather_Ball], ignore_index=True)

#3. Change columns names.
TCharged_Move = TCharged_Move.rename(columns={"Power":"CPower","Type":"CType", "Energy cost":"CEnergy cost", "Duration":"CDuration", "Damage window (s)":"Damage window start"})

#4 Change columns type.
TCharged_Move["Damage window start"] = TCharged_Move["Damage window start"].astype(float)



#Perform a left join between the "Pokemon_BTM" table and the "Charged_Move" table to identify the remaining unmatched Charged Moves.
Charged_Pokemon_BTMview = pd.merge(Pokemon_BTM, TCharged_Move, on='Charged Move', how='left')

#List of unmatched Pokémon between the tables. If "Charged_Pokemon_BTMview" returns any values, it is necessary to add the missing Pokémon to the table until the table is empty.
Charged_Pokemon_BTMview = Charged_Pokemon_BTMview[pd.isnull(Charged_Pokemon_BTMview["CType"])]
Charged_Pokemon_BTMview.groupby(['Charged Move'])['Charged Move'].count()



#Import the List of Combat Power Multiplier in order to extract the level variation from the excel sheet
CPM = pd.read_excel(r"C:\Users\joao\Desktop\POG\Pokemon_Data.xlsx", sheet_name="CPM")
CPM.head(10)
CPM.info()

#1. Remove unnecessary columns.
CPM = CPM.drop(columns={"stardust cost", "sd", "xl"})



#Create a column in the "Pokemon_BTM" table containing all 51 levels.
#1. Duplicate the data information 102 times in the specified table.
Pokemon_BTM['MovesID'] = range(1, len(Pokemon_BTM) + 1)
Pokemon_BTM = Pokemon_BTM.reindex(Pokemon_BTM.index.repeat(51)).reset_index(drop=True)

#2. Count the order of each unique duplicated value 
Pokemon_BTM['Level'] = Pokemon_BTM.groupby(Pokemon_BTM.index // 51).cumcount() + 1

#3. Divide the value of the level by 2
#Pokemon_BTM['Level'] = Pokemon_BTM['Level']/2

#4. Exclude the level 0.5
#Pokemon_BTM = Pokemon_BTM[Pokemon_BTM['Level'] != 0.5]

#TO BE REMOVED- #Merge Pokemon_BTM and CPM
#Pokemon_BTM["Level"] = 40.0


#Merge the "Pokemon_BTM" and "CPM" tables.
TPokemon_BTM = pd.merge(Pokemon_BTM, CPM, on='Level', how='left')



                    ##LEGACY IVs## - To be implemented in the future - ##LEGACY IVs##

#Add IVs per lv - Due to memory limitations, i could not implement the Details per custom IV. Maybe in the future i will implement this in other process using Azure or AWS services.

#from itertools import product
##
# Creating an index from 1 to 15
#indexIV = range(1, 16)

# Generating combinations with potential repetition in three columns
#combinationsIV = list(product(indexIV, repeat=3))

# Creating three separate lists for each column in the combinations
#column_1 = [comb[0] for comb in combinationsIV]
#column_2 = [comb[1] for comb in combinationsIV]
#column_3 = [comb[2] for comb in combinationsIV]

#IVs = {'AttackIV': column_1, 'DefenseIV': column_2, 'StaminaIV': column_3}
#IVs = pd.DataFrame(IVs)

#IVs = IVs[~((IVs['AttackIV'] == 15) & (IVs['DefenseIV'] == 15) & (IVs['StaminaIV'] == 15))]

#IVs = IVs.reindex(IVs.index.repeat(102)).reset_index(drop=True)
#IVs['Level'] = IVs.groupby(IVs.index // 102).cumcount() + 1
#IVs['Level'] = IVs['Level']/2
#IVs = IVs[IVs['Level'] != 0.5]
#IVs.head(103)
#IVs

#TPokemon_BTM = pd.merge(TPokemon_BTM, IVs, on='Level', how='left')
#TPokemon_BTM

##LEGACY IVs## - To be implemented in the future - ##LEGACY IVs##


#Update the columns in the "TPokemon_BTM" table based on each Pokémon's characteristics.
#1. Perform the calculation of the Combat Power (CP) for each Pokémon.
#1.1 Create columns for Individual Values (IVs) in the dataset.
TPokemon_BTM["AttackIV"] = 15
TPokemon_BTM["DefenseIV"] = 15
TPokemon_BTM["StaminaIV"] = 15

#1.2 Calculate the Combat Power (CP) for each Pokémon based on the provided information, including Individual Values (IVs).   CP = ((ATK+aIV)*((DEF+dIV)^0.5)*((HP+sIV)^0.5)*(CPM^2))/10 
TPokemon_BTM["CP"] = ((TPokemon_BTM["Attack"] + TPokemon_BTM["AttackIV"]) * ((TPokemon_BTM["Defense"] +TPokemon_BTM["DefenseIV"])**0.5) * ((TPokemon_BTM["HP"]+TPokemon_BTM["StaminaIV"])**0.5) * (TPokemon_BTM["CP Multiplier"]**2))/10
TPokemon_BTM["CP"]=TPokemon_BTM["CP"].astype(int)

#2. Update the "Attack" column by multiplying it by 20% for shadow Pokémon, and by 1 for non-shadow Pokémon.
TPokemon_BTM['Attack'] = TPokemon_BTM.apply(lambda row: 1.2 * row['Attack'] if row['is_shadow'] == True else row['Attack'], axis=1)

#3. Update the "Defense" column by multiplying it by (1/1.2) for shadow Pokémon, and by 1 for non-shadow Pokémon.
TPokemon_BTM['Defense'] = TPokemon_BTM.apply(lambda row: row['Defense'] * (1/1.2) if row['is_shadow'] == True else row['Defense'], axis=1)

#Perform a left join between the "TPokemon_BTM" table and the "TCharged_Move" and "TFast_Move" table.
TFPokemon_BTMCPM = pd.merge(TPokemon_BTM, TFast_Move, on='Fast Move', how='left')
TFCPokemon_BTMCPM = pd.merge(TFPokemon_BTMCPM, TCharged_Move, on='Charged Move', how='left')


#3.2 Calculate the true Attack, Defense and HP for each Pokémon based on the provided information, including Individual Values (IVs).   True STAT = (STAT + IV)*CPM | Note: if the STAT is bellow 1, the value needs to be rounded to 1
TFCPokemon_BTMCPM["Attack"] = np.floor((TFCPokemon_BTMCPM["Attack"] + TFCPokemon_BTMCPM["AttackIV"])*TFCPokemon_BTMCPM["CP Multiplier"])
TFCPokemon_BTMCPM.loc[TFCPokemon_BTMCPM["Attack"] < 1, "Attack"] = 1

TFCPokemon_BTMCPM["Defense"] = np.floor((TFCPokemon_BTMCPM["Defense"] + TFCPokemon_BTMCPM["DefenseIV"])*TFCPokemon_BTMCPM["CP Multiplier"])
TFCPokemon_BTMCPM.loc[TFCPokemon_BTMCPM["Defense"] < 1, "Defense"] = 1

TFCPokemon_BTMCPM["HP"] = np.floor((TFCPokemon_BTMCPM["HP"] + TFCPokemon_BTMCPM["StaminaIV"])*TFCPokemon_BTMCPM["CP Multiplier"])
TFCPokemon_BTMCPM.loc[TFCPokemon_BTMCPM["Defense"] < 1, "Defense"] = 1

#3.3 Calculate the true Attack Move for each move based on the provided information, including Same Type Attack Bonus (STAB). Update the Move if they have the type by multiplying it by 20%.
TFCPokemon_BTMCPM['Type1'] = TFCPokemon_BTMCPM['Type1'].str.title()
TFCPokemon_BTMCPM['Type2'] = TFCPokemon_BTMCPM['Type2'].str.title()

TFCPokemon_BTMCPM


####CSV Extract for Kaggle - Start
#Base Stats
#Pokemon_GO_csv = TFCPokemon_BTMCPM.copy()

#Pokemon_GO_csv= Pokemon_GO_csv[["ID","Pokemon Go","Type1","Type2", "HP","Attack","Defense","Level","CP Multiplier","CP"]]
#Pokemon_GO_csv = Pokemon_GO_csv.drop_duplicates()
#Pokemon_GO_csv

#Pokemon_GO_csv.to_csv(r'C:\Users\joao\Desktop\POG\extract\Pokemon_GO_Base_Stats.csv', index=False)

#Move combo

#Pokemon_GO_Move = TFCPokemon_BTMCPM.copy()
#Pokemon_GO_Move.info()
#Pokemon_GO_Move = Pokemon_GO_Move[["ID","Pokemon Go","Type1","Type2", "HP","Attack","Defense","Level","CP Multiplier","CP","Fast Move", "FPower", "FType", "FEnergy boost", "FDuration", "Charged Move", "CPower", "CType","CEnergy cost", "CDuration", "Damage window start"]]

#Pokemon_GO_Move.to_csv(r'C:\Users\joao\Desktop\POG\extract\Pokemon_GO_Details.csv', index=False)

####CSV Extract for Kaggle - end


TFCPokemon_BTMCPM["STABF"] = np.where((TFCPokemon_BTMCPM['FType'] == TFCPokemon_BTMCPM['Type1']) | (TFCPokemon_BTMCPM['FType'] == TFCPokemon_BTMCPM['Type2']), 1.2, 1)

TFCPokemon_BTMCPM["STABC"] = np.where((TFCPokemon_BTMCPM['CType'] == TFCPokemon_BTMCPM['Type1']) | (TFCPokemon_BTMCPM['CType'] == TFCPokemon_BTMCPM['Type2']), 1.2, 1)

#3.4 Calculate the Fast and Charged move Attack Move for each move based on each pokemon attack.     DMG = (0.5*Fpower*(ATK/DEF)*STAB)+1 | Note: The ATK refers to the Pokémon's attack, while DEF refers to the enemy's defense. We will assumed that enemy DEF = 200 | Formula by: https://gamepress.gg/pokemongo/how-calculate-comprehensive-dps
TFCPokemon_BTMCPM['Fdmg'] = (0.5 * TFCPokemon_BTMCPM['FPower'] * (TFCPokemon_BTMCPM['Attack']/200) * TFCPokemon_BTMCPM['STABF']) + 1

TFCPokemon_BTMCPM['Cdmg'] = (0.5 * TFCPokemon_BTMCPM['CPower'] * (TFCPokemon_BTMCPM['Attack']/200) * TFCPokemon_BTMCPM['STABC']) + 1

#3.5 Calculate the Energy per Second (EP) per Move for each move based on each pokemon attack.  CEPS100 = (CE+0.5FE+0.5y⋅CDWSC)/Dur | CEPS = CE/Dur | Note: If the Charged move has one bar, set Energy per Second (EPS) to 100 (CEPS100). If it has more than two bars, set EPS to CEPS. "y" represents the enemy's DPS, in this situation, we assume that the enemy doesn't attack | Formula by: https://gamepress.gg/pokemongo/how-calculate-comprehensive-dps
TFCPokemon_BTMCPM["CEPS100"] = (TFCPokemon_BTMCPM["CEnergy cost"] + (0.5*TFCPokemon_BTMCPM["FEnergy boost"]*1) + ((0.5*0)*TFCPokemon_BTMCPM["Damage window start"]))/TFCPokemon_BTMCPM["CDuration"]
TFCPokemon_BTMCPM["CEPS"] = TFCPokemon_BTMCPM["CEnergy cost"] / TFCPokemon_BTMCPM["CDuration"]

TFCPokemon_BTMCPM["CEPSTrue"] =TFCPokemon_BTMCPM.apply(lambda row: row["CEPS100"] if row['CEnergy cost'] == 100.0 else row["CEPS"], axis=1)

TFCPokemon_BTMCPM["FEPS"] = TFCPokemon_BTMCPM["FEnergy boost"]/TFCPokemon_BTMCPM["FDuration"]

#3.6 Calculate the DPS per Move for each move based on each pokemon attack.  DPS = dmg/Dur
TFCPokemon_BTMCPM["FDPS"] = TFCPokemon_BTMCPM['Fdmg'] / TFCPokemon_BTMCPM["FDuration"]
TFCPokemon_BTMCPM["CDPS"] = TFCPokemon_BTMCPM['Cdmg'] / TFCPokemon_BTMCPM["CDuration"]

#3.7 Calculate the DPS simple.  DPSs = (FDPS⋅CEPS+CDPS⋅FEPS)/(CEPS+FEPS) | Formula by: https://gamepress.gg/pokemongo/how-calculate-comprehensive-dps
TFCPokemon_BTMCPM["DPSs"] = ((TFCPokemon_BTMCPM["FDPS"]*TFCPokemon_BTMCPM["CEPSTrue"]) + (TFCPokemon_BTMCPM["CDPS"]*TFCPokemon_BTMCPM["FDPS"]))/(TFCPokemon_BTMCPM["CEPSTrue"]+TFCPokemon_BTMCPM["FDPS"])
TFCPokemon_BTMCPM

#Simple analysis

#1. Identify the Best Move type DPS.
SameMove = TFCPokemon_BTMCPM.copy()
SameMove.info()
SameMove["SameType"] = np.where(SameMove["FType"] == SameMove["CType"], SameMove["FType"], "Not the Same")
SameMove = SameMove[(SameMove["SameType"] != "Not the Same")]
SameMove
#Create an heatmap displaying the best dps per type (level 40)


import seaborn as sns
import matplotlib.pyplot as plt
heatmap = SameMove[(SameMove["Level"] == 40.0)]
heatmap = heatmap.groupby('SameType')['DPSs'].max()

# Create a dataframe with the top DPS for each type
heatmap = pd.DataFrame({'DPSs': heatmap.values}, index=heatmap.index)

# Create a heatmap with x and y switched
plt.figure(figsize=(12, 3))
sns.heatmap(heatmap.T, annot=True, cmap='Blues', fmt='.2f', linewidths=.5, cbar=False)
plt.title('Top DPS per Pokemon Type')
plt.xlabel('Type')
plt.show()
#We can view that the top three dps are Fying, Water and dragon. The botton 3 are Ice, Eletric and Bug


#Top 10 attacker DPS by Type
TOP_DPS = SameMove[(SameMove["Level"] == 51.0)]

TOP_DPS = TOP_DPS.loc[TOP_DPS.groupby(['Pokemon Go', "SameType"])['DPSs'].idxmax()][['MovesID', 'Pokemon Go', 'DPSs', "SameType"]]
TOP_DPS = TOP_DPS.loc[TOP_DPS.groupby('SameType')['DPSs'].nlargest(10).index.get_level_values(1)]

TOP_DPS = pd.merge(SameMove, TOP_DPS, on='MovesID', how='left')
TOP_DPS = TOP_DPS[TOP_DPS['SameType_y'].notnull()]

TOP_DPS = TOP_DPS[TOP_DPS["Level"] >= 25]

# TOP 10 FIRE  
FireMove = TOP_DPS[(TOP_DPS["SameType_x"] == "Fire")]
FireMove = FireMove.groupby(['Pokemon Go', "Level"])['DPSs_x'].max().reset_index()
FireMove

# Plotting the line chart using DataFrame columns directly
for pokemon, group_df in FireMove.groupby('Pokemon Go_x'):
    plt.plot(group_df['Level'], group_df['DPSs_x'], label=pokemon, marker='o', linestyle='-')

# Adding labels and title
plt.xlabel('Level')
plt.ylabel('DPS')
plt.title('DPS vs. Level for Pokémon Go')
plt.legend()
plt.show()
#We can view that the top three dps are Mega Blaziken, Shadow Chandelure and Mega Charizard.


#To be Continued.

#Thank you so much for taking the time to view my project. This project will continue and be improved in the future. The primary goal was to practice and enhance the Python skills acquired during the learning process.
#During my exploration on the internet, I found it challenging to discover a suitable dataset for basic exploration. As a result, I will share an extraction of this transformation on Kaggle to assist individuals interested in conducting their analysis on the theme.
#Hoping to receive feedback.. Happy coding! :)