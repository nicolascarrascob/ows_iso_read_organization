from owslib.iso import *

RESOURCES = '.'


def search_org(filepath, input_file_path, output_file_path):
    print('READING ' + input_file_path)
    file_input = open(input_file_path)
    file_output = open(output_file_path, 'w')
    file_output.write('docuuid;organization' + '\n')
    xml_files = file_input.readlines()
    for line in xml_files:
        filename = filepath + '/' + str(line.strip()) + '.txt'
        try:
            parser = etree.XMLParser(encoding="iso-8859-1")
            m = MD_Metadata(etree.parse(filename, parser))
            output_line = str(line.strip()) + ';' + m.contact[0].organization
            print(output_line)
            file_output.write(output_line + '\n')
        except:
            output_line = str(line.strip()) + ';' + 'error'
            print(output_line)
            file_output.write(output_line + '\n')
    file_output.close()


if __name__ == '__main__':
    xml_file_path = '%s/xml' % RESOURCES
    input_file_path = '%s/gpt_resources.txt' % RESOURCES
    output_file_path = '%s/gpt_resources_output.txt' % RESOURCES
    search_org(xml_file_path, input_file_path, output_file_path)
