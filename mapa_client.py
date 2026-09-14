import math,urllib.request,json
URL="https://mapa.ua/api/v1/current"
DNIPRO=(48.4647,35.0462)
def distance_km(a,b,c,d):
 r=6371.0
 p1,p2=math.radians(a),math.radians(c)
 dp,dl=math.radians(c-a),math.radians(d-b)
 x=math.sin(dp/2)**2+math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
 return 2*r*math.asin(math.sqrt(x))
def fetch_current(timeout=12):
 req=urllib.request.Request(URL,headers={"User-Agent":"dnipro-bot"})
 with urllib.request.urlopen(req,timeout=timeout) as r:
  return json.loads(r.read().decode())
def nearby_threats(data,lat,lon,radius_km=80):
 out=[]
 for o in data.get("objects") or []:
  if o.get("status")!="active": continue
  la,lo=o.get("lat"),o.get("lon")
  if la is None or lo is None: continue
  dist=distance_km(lat,lon,float(la),float(lo))
  if dist<=radius_km:
   item=dict(o); item["distance_km"]=round(dist,1); out.append(item)
 out.sort(key=lambda x:x["distance_km"]); return out
