fix_path = r'C:\Users\Edu\AstroHana\scripts\run_kmeans_final.py'
txt = open(fix_path, encoding='utf-8').read()
txt = txt.replace("distance_level=2,", "distance_level='euclidean',")
open(fix_path, 'w', encoding='utf-8').write(txt)
print('Fix aplicado OK')
