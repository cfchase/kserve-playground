import yaml

with open('coco.yaml', 'r') as f:
    data = yaml.safe_load(f)

coco_classes = [data['names'][i] for i in data['names']]
