p = r'C:\Users\Edu\AstroHana\scripts\vecinos.py'
t = open(p, encoding='utf-8').read()
t = t.replace("f'  {i:>2}  {nombre:<28}  {perfil:<35}  {str(anio):>4}  {dist:>10.2f}'",
              "f'  {i:>2}  {nombre:<28}  {perfil:<35}  {str(anio):>4}  {dist:>8.2f}'")
t = t.replace("f\"  {'#':>2}  {'Nombre':<28}  {'Perfil':<35}  {'Año':>4}  {'Dist ({nf}p)':>10}\"",
              "f\"  {'#':>2}  {'Nombre':<28}  {'Perfil':<35}  {'Año':>4}  {f'Dist({nf}p)':>10}\"")
open(p, 'w', encoding='utf-8').write(t)
print('OK')
