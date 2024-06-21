import pandas as pd
import tkinter as tk
from tkcalendar import Calendar



df = pd.read_csv('C:\Python Projects\Stock Management\Reagent_Tracker.csv')


# First Tkinter window with Dropdown widget for reagent, Entry widget for lot
root = tk.Tk()
root.title('Stock Management Input')
root.minsize(500, 300)

dropdown_instructions = tk.Label(root, text = 'Select the reagent from the dropdown menu: ')
dropdown_instructions.grid(row = 0, column = 1)
selected_option = tk.StringVar()
dropdown = tk.OptionMenu(root, selected_option, 'Illumina DNA PCR-free Purification Kit, 96 samples', 'Illumina DNA PCR-free Tagmentation Kit, 96 samples', 'IUO, 32 INDEX UDI, PCR-FREE', 'SBS', 'Cluster', 'Buffer', 'FC')
dropdown.grid(row = 2, column = 1, pady = 20)




def return_choices():
    global lot_entry
    global lot
    global expiry_date
    global cal
    global quantity_input
    global quantity
    global selected_option
    global site_input
    

    selected_option = selected_option.get()
    lot = lot_entry.get()
    expiry_date = cal.selection_get()
    quantity = quantity_input.get()
    site_input = site_input.get()
   
    

lot_instructions = tk.Label(root, text= 'Input Lot Number: ')
lot_instructions.grid(row =3, column = 0)

lot_entry = tk.Entry(root)
lot_entry.grid(row = 3, column = 1)

quantity_instructions = tk.Label(root, text = 'Select quantity: ')
quantity_instructions.grid (row = 5, column = 0)

quantity_input = tk.Spinbox(root, from_= 1, to=100)
quantity_input.grid(row = 5, column = 1, pady=20)

site_instructions = tk.Label(root, text = 'Select Site:')
site_instructions.grid(row = 6, column = 0 )

site_input = tk.StringVar()
Hx = tk.Radiobutton(root, text = 'Hx', variable = site_input, value = 'Hx')
IC = tk.Radiobutton (root, text = 'IC', variable = site_input, value = 'IC')
site_input.set('Hx')

Hx.grid (row = 7, column = 1)

IC.grid (row = 7, column = 2)


expiry_instructions = tk.Label(root, text = 'Select expiry date: ')
expiry_instructions.grid(row = 8, column = 0, pady = 10)

cal = Calendar(root, selectmode = 'day')
cal.grid(row = 9, column = 1, pady = 10)



def declare_FC():
     global FC_lot_entry
     FC_lot_entry = FC_lot_entry.get()

def if_FC():
    
    global selected_option
    global FC_lot_entry

    if selected_option == 'FC':
        main = tk.Tk()
        main.geometry("300x200")
        main.title("FC input")
        FC_lot_instructions = tk.Label(main, text = 'Input Inner Box Lot: ')
        FC_lot_instructions.grid(row = 0, column = 0, pady = 10)

        FC_lot_entry = tk.Entry(main)
        FC_lot_entry.grid(row = 0, column = 1)

        submit_button = tk.Button(main, text = 'Submit', command = lambda:[declare_FC(), main.destroy() ])
        submit_button.grid (row = 10, column = 1, pady=10)
        main.mainloop()
        
        
    

submit_button = tk.Button(root, text = 'Submit', command = lambda:[return_choices(), root.destroy(), if_FC()])
submit_button.grid (row = 10, column = 1, pady=10)
        
root.mainloop()


# Check lot format

if selected_option == 'Illumina DNA PCR-free Purification Kit, 96 samples':
    if (len(lot) == 9 and lot[0] == 'A' and lot[7:9] == '-1'):
                print('Valid Purification Lot')
    else: 
        print('Invalid Purification Lot!')
        exit()
        
if selected_option == 'Illumina DNA PCR-free Tagmentation Kit, 96 samples':
    if (len(lot) == 9 and lot[0] == 'A' and lot[7:9] == '-2'):
                print('Valid Tagmentation Lot')
    else: 
        print('Invalid Tagmentation Lot!')
        exit()

if selected_option == 'IUO, 32 INDEX UDI, PCR-FREE':
    if len(lot) == 9 and str(lot).isdigit():
         print ('Valid UDI lot')
    else: 
        print('Invalid UDI Lot!')
        exit()

if selected_option == 'SBS':
    if len(lot) == 8 and str(lot).isdigit():
         print ('Valid SBS lot')
    else: 
        print('Invalid SBS Lot!')
        exit()

if selected_option == 'Cluster':
    if len(lot) == 8 and str(lot).isdigit():
         print ('Valid Cluster lot')
    else: 
        print('Invalid Cluster Lot!')
        exit()
    
if selected_option == 'Buffer':
    if len(lot) == 8 and str(lot).isdigit():
         print ('Valid Buffer lot')
    else: 
        print('Invalid Buffer Lot!')
        exit()

if selected_option == 'FC':
    if len(lot) == 8 and str(lot).isdigit():
         print ('Valid FC lot')
    else: 
        print('Invalid FC Lot!')
        exit()





# Check if expiry date in past?

# Append to FC csv

if selected_option == 'FC':

    FC_df = pd.read_csv('C:\Python Projects\Stock Management\Combined Reagent Tracker\FC_Crossref.csv')
    box_lot_list = FC_df['Box_Lot'].tolist()
    SF_lot_list = FC_df['SF_Lot'].tolist()

    if int(lot) in box_lot_list:
        pass

    else:
        new_row_FC_df = pd.DataFrame({'Box_Lot': [lot], 'SF_Lot': [FC_lot_entry]})
        FC_df = pd.concat([FC_df, new_row_FC_df], ignore_index=True)
        FC_df.drop_duplicates(subset = None, inplace = True)
        print('FC lot added')

        FC_df.to_csv('C:\Python Projects\Stock Management\Combined Reagent Tracker\FC_Crossref.csv', index= False)





# Proccess input

df = pd.read_csv('C:\Python Projects\Stock Management\Combined Reagent Tracker\Reagent_Tracker.csv')


# Check if existing and add to

i=0
for df_lot in df['Lot']:
    if df.loc[i, 'Site'] == site_input:
        if df_lot == lot or df_lot[0:7] == lot:
            if df.loc[i, 'Reagent'] == selected_option:
                
                original_quantity = int(df.loc[i, 'Quantity'])
                df.loc[i, 'Quantity'] = original_quantity + int(quantity)

                i='Already added'
                break
            else:
                i+=1
        else:
            i+=1


# If not existing:
        
if i != 'Already added':
    
    new_row_df = pd.DataFrame({'Reagent': [selected_option], 'Lot': [lot], 'Quantity': [quantity], 'Expiry Date': [expiry_date], 'Site': [site_input]})
    df = pd.concat([df, new_row_df], ignore_index=True)
    
else:
    pass


df.to_csv('C:\Python Projects\Stock Management\Combined Reagent Tracker\Reagent_Tracker.csv', index = False)




