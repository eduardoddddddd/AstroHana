fix = open(r'C:\Users\Edu\AstroHana\scripts\run_kmeans_hanaml.py').read()
fix = fix.replace("init='kmeans++'", "init='patent'")
open(r'C:\Users\Edu\AstroHana\scripts\run_kmeans_hanaml.py','w').write(fix)
print('OK')
