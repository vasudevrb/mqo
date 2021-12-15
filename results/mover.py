folders = ['00', '43', '53', '55', '57', '58', '61', '63', '66', '70', '75', '80', '83', '87', '90', '93', '97']

def modify_templates(file_name):
    f = open(file_name, 'r')
    lines = f.readlines()
    f.close()

    new_lines = lines.copy()
    new_lines[77:77] = lines[4:18]
    del new_lines[4:18]

    f2 = open(file_name, 'w')
    for line in new_lines:
        f2.write(line)
    f2.close()

def modify_values(file_name):
    f = open(file_name, 'r')
    lines = f.readlines()
    f.close()

    new_lines = lines.copy()
    new_lines[10:10] = lines[0:2]
    del new_lines[0:2]

    f2 = open(file_name, 'w')
    for line in new_lines:
        f2.write(line)
    f2.close()

for fol in folders:
    modify_templates('../resources/der/' + fol + '/query_templates.txt')
    modify_values('../resources/der/' + fol + '/query_template_md.txt')