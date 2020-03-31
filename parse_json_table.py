import json
import csv

# AWS script modifed by Ron Hayden

def get_rows_columns_map(table_result, blocks_map):
    rows = {}
    for relationship in table_result['Relationships']:
        if relationship['Type'][0] == 'CHILD':
            for child_id in relationship['Ids']:
                cell = blocks_map[child_id]
                if cell['BlockType'][0] == 'CELL':
                    row_index = cell['RowIndex'][0]
                    col_index = cell['ColumnIndex'][0]
                    if row_index not in rows:
                        # create new row
                        rows[row_index] = {}
                        
                    # get the text value
                    rows[row_index][col_index] = get_text(cell, blocks_map)
    return rows

def get_text(result, blocks_map):
    text = ''
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'][0] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map[child_id]
                    if word['BlockType'][0] == 'WORD':
                        text += word['Text'][0] + ' '
                    if word['BlockType'][0] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'][0] =='SELECTED':
                            text +=  'X '
    

    # Clean up the text.
    text = text.replace('$', '')   # $s are often misplaced, and are not needed.
    text = text.strip()            # Remove whitespace from both ends.
    
    # Commas and decimals get confused by OCR and are not needed so if this is a numerical value just remove them.
    # Try removing commas and decimals and see if resulting value is a pure number (don't want to remove them from text values).
    stripped = text.replace(',', '').replace('.', '')
    
    # There may be parenthesis for a negative number which defeats the numeric check, so remove them just for the check.
    no_parens = stripped.replace('(', '').replace(')', '')
    
    if no_parens.isnumeric():
        text = stripped
    return text

def generate_table(csv_writer, table_result, blocks_map, table_index):
    rows = get_rows_columns_map(table_result, blocks_map)

    table_id = 'Table_' + str(table_index)
    
    # get cells.
    csv_writer.writerow([f'Table: {table_id}'])

    # TODO: Not clear that items() is needed, can simplify by just using values()?
    for row_index, cols in rows.items():
        row = []
        for col_index, text in cols.items():
            row.append(text)
        csv_writer.writerow(row)


def parse_file(path, csv_writer):
    print(f'Loading {path}...')
    response = json.load(open(path))

    # Get the text blocks
    blocks=response

    blocks_map = {}
    table_blocks = []
    for block in blocks:
        block_id = block['Id'][0]
        blocks_map[block_id] = block
        if block['BlockType'][0] == "TABLE":
            table_blocks.append(block)

    if len(table_blocks) <= 0:
        exit("Error: No tables found")

    for index, table in enumerate(table_blocks):
        generate_table(csv_writer, table, blocks_map, index +1)
        csv_writer.writerow([' '])
        csv_writer.writerow([' '])

def main():
    import sys
    if len(sys.argv) < 3:
        raise Exception("Error: Please provide an input path and an output path!")

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    
    with open(output_path, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        parse_file(input_path, csv_writer)
    
    print(f"Output written to {output_path}!")
        

if __name__ == '__main__':
    main()
