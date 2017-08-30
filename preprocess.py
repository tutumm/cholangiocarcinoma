with open('bigdata_payment/0005_AA.TXT') as f:
  content = f.readlines()

newContent = []
  
for line in content:
  if(line[0] == '|'):
    if(len(line.split('|'))!=3):
      if( line.split('|')[4].strip() != 'BP'):
        newContent.append(line) 

with open('new_content.txt','w') as f:
  for line in newContent:
    f.write(line)