"""Independent public-ATS adapters that extend Remote4Me's existing five-ATS crawler.

The main crawler already handles Ashby, Greenhouse, Lever, Workable and
SmartRecruiters. This extension adds Workday, Recruitee, Teamtailor, Pinpoint
and Keka, writing into the same four country/workplace tabs.
"""
import json, re, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
import requests
import crawler

UA = "Remote4.me Extended ATS Collector/1.0 (+https://remote4.me)"
TIMEOUT, RETRIES, WORKERS = 45, 3, 8
MAX_CDX_PAGES, MAX_BOARDS_PER_SOURCE, MAX_AGE_DAYS = 5, 250, 7
SOURCES = {
    "workday": "myworkdayjobs.com",
    "recruitee": "recruitee.com",
    "teamtailor": "teamtailor.com",
    "pinpoint": "pinpointhq.com",
    "keka": "keka.com",
}
RESERVED = {"www","app","api","auth","blog","docs","help","jobs","status","support","careers","career","mail","admin"}
S = requests.Session()
S.headers.update({"User-Agent": UA, "Accept":"application/json,text/plain,*/*", "Accept-Language":"en-US,en;q=0.9"})

def request(url, params=None, method="GET", payload=None, text_only=False, timeout=TIMEOUT):
    last = None
    for attempt in range(RETRIES):
        try:
            r = S.request(method, url, params=params, json=payload, timeout=timeout)
            if r.status_code == 200:
                if text_only: return r.text
                try: return r.json()
                except ValueError: return r.text
            if r.status_code not in {408,425,429,500,502,503,504}: return None
            last = f"HTTP {r.status_code}"
        except requests.RequestException as e: last = str(e)
        if attempt < RETRIES-1: time.sleep(2**attempt)
    print(f"Request failed {url}: {last}")
    return None

def clean(v):
    if v is None: return ""
    if isinstance(v,(dict,list)): return json.dumps(v,ensure_ascii=False)
    return re.sub(r"\\s+"," ",re.sub(r"<[^>]+>"," ",str(v))).strip()

def safe_url(v):
    v=clean(v)
    return v if v.startswith(("https://","http://")) else ""

def parse_date(v):
    if not v: return ""
    try: return datetime.fromisoformat(str(v).replace("Z","+00:00")).date().isoformat()
    except Exception:
        try: return datetime.strptime(str(v)[:10],"%Y-%m-%d").date().isoformat()
        except Exception: return ""

def salary(minv=None,maxv=None,currency="",period=""):
    if minv is None and maxv is None: return ""
    def n(x):
        try: return f"{float(x):,.0f}"
        except Exception: return str(x)
    val=f"{currency} {n(minv) if minv is not None else n(maxv)}" if minv is None or maxv is None else f"{currency} {n(minv)}–{n(maxv)}"
    return (val+" / "+str(period)).strip(" /") if period else val.strip()

def normalize(source,company,raw,title,location,job_id,job_url,apply_url="",posted="",description="",employment="",pay="",department="",team="",remote_hint=False,workplace_type=None):
    title,location=clean(title),clean(location)
    if not title or not safe_url(job_url) or not crawler.title_is_relevant(title): return None
    workplace=crawler.classify_workplace(location=location,workplace_type=workplace_type,remote_flag=remote_hint,structured_data=raw or {})
    country_code=(raw or {}).get("countryCode") or (raw or {}).get("country_code") or (raw or {}).get("country") or ""
    country=crawler.classify_country(location=location,country_code=clean(country_code),address_country=clean((raw or {}).get("countryName")),description=clean(description),workplace=workplace)
    if not crawler.location_is_allowed(country,workplace): return None
    posted=parse_date(posted)
    if posted:
        try:
            if datetime.strptime(posted,"%Y-%m-%d").date() < datetime.now(timezone.utc).date()-timedelta(days=MAX_AGE_DAYS): return None
        except ValueError: pass
    return {"ats":source,"company":clean(company),"job_id":clean(job_id),"title":title,"location":location,"country":country,"workplace":workplace,"posted_date":posted,"job_url":safe_url(job_url),"apply_url":safe_url(apply_url) or safe_url(job_url),"description":clean(description),"employment_type":clean(employment),"salary":clean(pay),"department":clean(department),"team":clean(team)}

def workday(slug,company):
    try: tenant,host,site=slug.split("/")
    except ValueError: return []
    root=f"https://{tenant}.{host}.myworkdayjobs.com/{site}"
    api=f"https://{tenant}.{host}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/jobs"
    out=[]
    for page in range(10):
        d=request(api,method="POST",payload={"appliedFacets":{},"limit":20,"offset":page*20,"searchText":""})
        rows=d.get("jobPostings",[]) if isinstance(d,dict) else []
        for x in rows:
            ext=clean(x.get("externalPath"))
            if not ext: continue
            jid=re.sub(r"[^\\w-]","-",ext.rstrip("/").split("/")[-1])[-64:]
            rt=clean(x.get("remoteType"))
            j=normalize("workday",company,x,x.get("title"),x.get("locationsText"),jid,root+ext,root+ext,description="",remote_hint=bool(re.search(r"remote|hybrid",rt,re.I)),workplace_type=rt)
            if j: out.append(j)
        if len(rows)<20: break
    return out

def recruitee(slug,company):
    d=request(f"https://{slug}.recruitee.com/api/offers/")
    rows=d.get("offers",[]) if isinstance(d,dict) else []
    out=[]
    for x in rows:
        if x.get("status") and x["status"]!="published": continue
        loc=x.get("location") or {}
        location=", ".join(str(v) for v in (loc.get("city"),loc.get("region"),loc.get("country")) if v) if isinstance(loc,dict) else clean(loc)
        desc=" ".join((clean(x.get("description")),clean(x.get("requirements"))))
        sal=x.get("salary") or {}
        j=normalize("recruitee",company,x,x.get("title"),location,x.get("id"),x.get("careers_url") or x.get("url"),x.get("careers_apply_url") or x.get("url"),x.get("published_at") or x.get("created_at"),desc,x.get("employment_type_code") or x.get("employment_type"),salary(sal.get("min"),sal.get("max"),sal.get("currency",""),sal.get("period","")),x.get("department"),remote_hint=x.get("remote",False))
        if j: out.append(j)
    return out

def teamtailor(slug,company):
    d=request(f"https://{slug}.teamtailor.com/jobs.json")
    rows=d.get("items",[]) if isinstance(d,dict) else []
    out=[]
    for x in rows:
        jp=x.get("_jobposting") or {}
        places=jp.get("jobLocation") or []
        if isinstance(places,dict): places=[places]
        locs=[]
        for p in places:
            a=p.get("address") or {}
            locs.extend([a.get("addressLocality"),a.get("addressRegion"),a.get("addressCountry")])
        location=", ".join(str(v) for v in locs if v)
        desc=clean(x.get("content_html") or jp.get("description"))
        hint=bool(re.search(r"remote",clean(x.get("title"))+" "+clean(jp.get("jobLocationType")),re.I))
        j=normalize("teamtailor",company,x,x.get("title"),location,x.get("id"),x.get("url"),x.get("url"),jp.get("datePosted") or x.get("date_published"),desc,jp.get("employmentType"),remote_hint=hint)
        if j: out.append(j)
    return out

def pinpoint(slug,company):
    d=request(f"https://{slug}.pinpointhq.com/postings.json")
    rows=d.get("data",[]) if isinstance(d,dict) else []
    out=[]
    for x in rows:
        loc=x.get("location") or {}
        location=clean(loc.get("name") or loc.get("city"))
        rt=x.get("workplace_type_text") or x.get("workplace_type") or ""
        pay=salary(x.get("compensation_minimum"),x.get("compensation_maximum"),x.get("compensation_currency",""),x.get("compensation_frequency",""))
        dept=x.get("job") or {}
        dept=dept.get("department") or {}
        j=normalize("pinpoint",company,x,x.get("title"),location,x.get("id"),x.get("url"),x.get("url"),description=x.get("description"),employment=x.get("employment_type_text") or x.get("employment_type"),pay=pay,department=dept.get("name") if isinstance(dept,dict) else dept,remote_hint=bool(re.search(r"remote|hybrid",clean(rt),re.I)),workplace_type=rt)
        if j: out.append(j)
    return out

def keka(slug,company):
    root=f"https://{slug}.keka.com/careers/"
    html=request(root,text_only=True)
    if not isinstance(html,str): return []
    m=re.search(r"/ats/documents/([0-9a-f-]{36})/",html,re.I)
    if not m: return []
    rows=request(f"{root}api/embedjobs/default/active/{m.group(1)}")
    if not isinstance(rows,list): return []
    out=[]
    for x in rows:
        places=x.get("jobLocations") or []
        p=places[0] if places else {}
        location=", ".join(str(v) for v in (p.get("city") or p.get("name"),p.get("countryName")) if v)
        url=f"{root}jobdetails/{x.get('id')}"
        sal=x.get("salaryRange") or {}
        pay=x.get("salaryRangeFormat") or salary(sal.get("minimum"),sal.get("maximum"),sal.get("currency",""))
        et={1:"Part-time",2:"Full-time"}.get(x.get("jobType"),"")
        j=normalize("keka",company,x,x.get("title"),location,x.get("id"),url,url,x.get("publishedOn"),x.get("description"),et,pay,x.get("departmentName"),remote_hint=bool(x.get("isRemote") or x.get("remote")))
        if j: out.append(j)
    return out

FETCH={"workday":workday,"recruitee":recruitee,"teamtailor":teamtailor,"pinpoint":pinpoint,"keka":keka}

def cdx_candidates(source,index):
    domain=SOURCES[source]
    params={"url":f"*.{domain}","output":"json","fl":"url","showNumPages":"true"}
    meta=request(index,params=params)
    pages=min(int(meta.get("pages",1)),MAX_CDX_PAGES) if isinstance(meta,dict) else 1
    found=set()
    for page in range(pages):
        try:
            r=S.get(index,params={"url":f"*.{domain}","output":"json","fl":"url","page":page},timeout=90)
            if r.status_code!=200:
                print(f"Common Crawl {source} page {page+1}: HTTP {r.status_code}")
                continue
            for line in r.text.splitlines():
                try:
                    u=urlparse(json.loads(line).get("url",""))
                    host=(u.hostname or "").lower()
                    parts=[p for p in u.path.split("/") if p]
                    if source=="workday":
                        suffix=".myworkdayjobs.com"
                        if not host.endswith(suffix): continue
                        labels=host[:-len(suffix)].split(".")
                        if len(labels)!=2 or not re.fullmatch(r"wd\\d+",labels[1]): continue
                        site=parts[1] if parts and re.fullmatch(r"[a-z]{2}-[a-z]{2}",parts[0],re.I) and len(parts)>1 else (parts[0] if parts else "")
                        if site: found.add(f"{labels[0]}/{labels[1]}/{site}")
                    else:
                        suffix="."+domain
                        if not host.endswith(suffix): continue
                        sub=host[:-len(suffix)]
                        if "." in sub or sub in RESERVED or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}",sub): continue
                        if source=="keka" and not u.path.lower().startswith("/careers"): continue
                        found.add(sub)
                except Exception: continue
        except requests.RequestException as e: print(f"Common Crawl {source}: {e}")
    return found

def discover():
    meta=request("https://index.commoncrawl.org/collinfo.json")
    index=meta[0].get("cdx-api") if isinstance(meta,list) and meta else None
    result={s:set() for s in SOURCES}
    if index:
        for source in SOURCES:
            result[source]=cdx_candidates(source,index)
            print(f"Discovered {len(result[source])} {source} boards")
    # Reuse cached extended-provider boards if a user has added them to Boards.
    try:
        ss=crawler.get_spreadsheet()
        ws=crawler.get_or_create_worksheet(ss,"Boards",rows=10000,cols=8)
        rows=ws.get_all_values()
        if rows:
            h=rows[0]
            ai=h.index("ATS") if "ATS" in h else -1
            si=h.index("Board Slug") if "Board Slug" in h else -1
            xi=h.index("Active") if "Active" in h else -1
            for row in rows[1:]:
                if ai>=0 and si>=0 and len(row)>max(ai,si):
                    src,slug=row[ai].strip().lower(),row[si].strip()
                    active=xi<0 or len(row)<=xi or row[xi].strip().lower() in {"yes","true","1"}
                    if src in result and slug and active: result[src].add(slug)
    except Exception as e: print(f"Boards cache read skipped: {e}")
    return {s:sorted(v,key=str.lower)[:MAX_BOARDS_PER_SOURCE] for s,v in result.items()}

def col(n):
    out=""
    while n:
        n,r=divmod(n-1,26); out=chr(65+r)+out
    return out

BASE=["Date","Company","Job Title","Location","Country","Posted Date","Job Link","Job ID","ATS"]
DETAIL=["Workplace","Employment Type","Salary","Department","Team","Job Description","Apply Link"]

def append_to_tab(ws,jobs,tab):
    values=ws.get_all_values()
    if not values:
        headers=BASE+DETAIL
        ws.update(f"A1:{col(len(headers))}1",[headers],value_input_option="RAW")
        values=[headers]
    else:
        headers=[clean(x) for x in values[0]]
        # Preserve the current nine columns and append detail columns to the right.
        for h in DETAIL:
            if h not in headers: headers.append(h)
        if headers != [clean(x) for x in values[0]]:
            ws.update(f"A1:{col(len(headers))}1",[headers],value_input_option="RAW")
    idx={h:i for i,h in enumerate(headers)}
    ids,urls,fallback=set(),set(),set()
    for row in values[1:]:
        def cell(h):
            i=idx.get(h,-1)
            return row[i] if 0<=i<len(row) else ""
        ats,jid=clean(cell("ATS")).lower(),clean(cell("Job ID")).lower()
        url=crawler.normalize_url(cell("Job Link"))
        fb=(clean(cell("Company")).lower(),clean(cell("Job Title")).lower(),clean(cell("Location")).lower())
        if ats and jid: ids.add((ats,jid))
        if url: urls.add(url)
        if all(fb): fallback.add(fb)
    today=datetime.now(timezone.utc).date().isoformat()
    new=[]
    for j in jobs:
        ats,jid=clean(j.get("ats")).lower(),clean(j.get("job_id")).lower()
        url=crawler.normalize_url(j.get("job_url"))
        fb=(clean(j.get("company")).lower(),clean(j.get("title")).lower(),clean(j.get("location")).lower())
        if (ats and jid and (ats,jid) in ids) or (url and url in urls) or (all(fb) and fb in fallback): continue
        fields={"Date":today,"Company":j["company"],"Job Title":j["title"],"Location":j["location"],"Country":j["country"],"Posted Date":j["posted_date"],"Job Link":j["job_url"],"Job ID":j["job_id"],"ATS":j["ats"],"Workplace":j["workplace"].title(),"Employment Type":j["employment_type"],"Salary":j["salary"],"Department":j["department"],"Team":j["team"],"Job Description":j["description"],"Apply Link":j["apply_url"]}
        row=[""]*len(headers)
        for k,v in fields.items():
            if k in idx: row[idx[k]]=clean(v)
        new.append(row)
        if ats and jid: ids.add((ats,jid))
        if url: urls.add(url)
        if all(fb): fallback.add(fb)
    if new: ws.append_rows(new,value_input_option="RAW")
    print(f"{tab}: {len(new)} new extended-ATS jobs")
    return len(new)

def main():
    print("REMOTE4.ME EXTENDED ATS — WORKDAY / RECRUITEE / TEAMTAILOR / PINPOINT / KEKA")
    boards=discover()
    tasks=[(s,slug) for s,slugs in boards.items() for slug in slugs]
    print(f"Extended ATS boards queued: {len(tasks)}")
    jobs=[]
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures={pool.submit(FETCH[s],slug,re.sub(r"[-_]+"," ",slug.split("/")[0] if s=="workday" else slug.replace(".na","")).title()):(s,slug) for s,slug in tasks}
        for f in as_completed(futures):
            source,slug=futures[f]
            try:
                found=f.result()
                jobs.extend(found)
                print(f"{source}/{slug}: {len(found)} eligible")
            except Exception as e: print(f"ERROR {source}/{slug}: {e}")
    unique={}
    for j in jobs:
        if crawler.is_excluded_company(j["company"],j["job_url"]): continue
        key=(j["ats"],j["job_id"] or crawler.normalize_url(j["job_url"]))
        unique.setdefault(key,j)
    jobs=list(unique.values())
    print(f"Unique eligible extended-ATS jobs: {len(jobs)}")
    ss=crawler.get_spreadsheet()
    grouped={key:[] for key in crawler.OUTPUT_TABS}
    for j in jobs:
        key=(j["country"],j["workplace"])
        if key in grouped: grouped[key].append(j)
    total=0
    for key,tab in crawler.OUTPUT_TABS.items():
        ws=crawler.get_or_create_worksheet(ss,tab,rows=10000,cols=len(BASE+DETAIL))
        total+=append_to_tab(ws,grouped[key],tab)
    print(f"Done. Added {total} jobs to the existing four tabs. No WordPress publishing.")

if __name__=="__main__": main()
