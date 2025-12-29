import os
import shutil
import zipfile
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from sqlalchemy import create_engine, Column, String, Float, Date, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

app = FastAPI()

# --- 1. BANCO DE DADOS ---
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./test.db")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class NFe(Base):
    # MUDEI O NOME DA TABELA PARA IGNORAR DADOS VELHOS QUE ESTAVAM DANDO ERRO
    __tablename__ = "notas_fiscais_v2" 
    
    chave_item = Column(String, primary_key=True, index=True) # ID Único (Chave + Numero Item)
    chave_acesso = Column(String)
    ano = Column(String)
    valor_total_nota = Column(Float)
    valor_item = Column(Float)
    dados_json = Column(String) # Guarda todos os dados para o Excel

# Cria a tabela nova
Base.metadata.create_all(bind=engine)

# --- 2. FUNÇÕES ÚTEIS ---
ns_map = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

def pegar_valor(no, caminho, tipo=str):
    if no is None: return tipo(0) if tipo in [float, int] else ""
    try:
        # Tenta achar com namespace
        r = no.find(caminho, ns_map)
        # Se falhar, tenta sem namespace (alguns XMLs variam)
        if r is None: r = no.find(caminho.replace('nfe:', ''))
        
        if r is not None and r.text:
            val = r.text.replace(',', '.')
            return tipo(val)
        return tipo(0) if tipo in [float, int] else ""
    except:
        return tipo(0) if tipo in [float, int] else ""

def formatar_data(data_obj):
    if not data_obj: return ""
    return data_obj.strftime('%d/%m/%Y')

def extrair_dados_xml(arq):
    try:
        tree = ET.parse(arq)
        root = tree.getroot()
        
        # Localiza o infNFe
        if 'nfeProc' in root.tag:
            inf_nfe = root.find('.//nfe:infNFe', ns_map)
        else:
            inf_nfe = root.find('nfe:infNFe', ns_map)
            
        if inf_nfe is None: return []

        # Blocos principais
        ide = inf_nfe.find('nfe:ide', ns_map)
        emit = inf_nfe.find('nfe:emit', ns_map)
        dest = inf_nfe.find('nfe:dest', ns_map)
        total = inf_nfe.find('.//nfe:ICMSTot', ns_map)
        
        # Chave e Data
        chave = pegar_valor(root.find('.//nfe:infProt', ns_map), 'nfe:chNFe')
        if not chave: chave = inf_nfe.attrib.get('Id', '')[3:]

        data_raw = pegar_valor(ide, 'nfe:dhEmi') or pegar_valor(ide, 'nfe:dEmi')
        data_dt = datetime.now().date()
        if len(data_raw) >= 10:
            data_dt = datetime.strptime(data_raw[:10], '%Y-%m-%d').date()

        v_nf = pegar_valor(total, 'nfe:vNF', float)

        # Loop nos Itens
        itens_db = []
        dets = inf_nfe.findall('nfe:det', ns_map)
        
        for i, det in enumerate(dets):
            prod = det.find('nfe:prod', ns_map)
            v_prod = pegar_valor(prod, 'nfe:vProd', float)
            
            # MONTAGEM DO EXCEL (DICIONARIO)
            item_dict = {
                'Mês': str(data_dt.month).zfill(2),
                'Ano': str(data_dt.year),
                'Chave Acesso NFe': chave,
                'Inscrição Destinatário': pegar_valor(dest, 'nfe:IE'),
                'Inscrição Emitente': pegar_valor(emit, 'nfe:IE'),
                'Razão Social Emitente': pegar_valor(emit, 'nfe:xNome'),
                'Cnpj Emitente': pegar_valor(emit, 'nfe:CNPJ'),
                'UF Emitente': pegar_valor(emit, 'nfe:enderEmit/nfe:UF'),
                'Nr NFe': pegar_valor(ide, 'nfe:nNF'),
                'Série': pegar_valor(ide, 'nfe:serie'),
                'Data NFe': formatar_data(data_dt),
                'BC ICMS Total': pegar_valor(total, 'nfe:vBC', float),
                'ICMS Total': pegar_valor(total, 'nfe:vICMS', float),
                'BC ST Total': pegar_valor(total, 'nfe:vBCST', float),
                'ICMS ST Total': pegar_valor(total, 'nfe:vST', float),
                'Desc Total': pegar_valor(total, 'nfe:vDesc', float),
                'IPI Total': pegar_valor(total, 'nfe:vIPI', float),
                'Total Produtos': pegar_valor(total, 'nfe:vProd', float),
                'Total NFe': v_nf,
                'Descrição Produto NFe': pegar_valor(prod, 'nfe:xProd'),
                'NCM na NFe': pegar_valor(prod, 'nfe:NCM'),
                'CFOP NFe': pegar_valor(prod, 'nfe:CFOP'),
                'Qtde': pegar_valor(prod, 'nfe:qCom', float),
                'Unid': pegar_valor(prod, 'nfe:uCom'),
                'Vr Unit': pegar_valor(prod, 'nfe:vUnCom', float),
                'Vr Total': v_prod,
                'Desconto Item': pegar_valor(prod, 'nfe:vDesc', float)
            }
            
            # Objeto para salvar no Banco
            novo_obj = NFe(
                chave_item=f"{chave}-{i+1}", # Chave Única composta
                chave_acesso=chave,
                ano=str(data_dt.year),
                valor_total_nota=v_nf,
                valor_item=v_prod,
                dados_json=str(item_dict)
            )
            itens_db.append(novo_obj)
            
        return itens_db

    except Exception as e:
        print(f"Erro XML: {e}")
        return []

# --- 3. ROTAS ---

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    temp_dir = "temp_files"
    if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)
    
    caminho_zip = os.path.join(temp_dir, "temp.zip")
    with open(caminho_zip, "wb") as buffer: shutil.copyfileobj(file.file, buffer)
    
    try:
        with zipfile.ZipFile(caminho_zip, 'r') as z: z.extractall(temp_dir)
    except:
        return JSONResponse({"sucesso": False, "msg": "Erro: Arquivo não é ZIP"}, 400)
    
    arquivos = glob.glob(f"{temp_dir}/**/*.xml", recursive=True)
    arquivos += glob.glob(f"{temp_dir}/**/*.XML", recursive=True)
    
    session = SessionLocal()
    cont = 0
    try:
        for f in arquivos:
            itens = extrair_dados_xml(f)
            for it in itens:
                session.merge(it)
                cont += 1
        session.commit()
    except Exception as e:
        session.rollback()
        return JSONResponse({"sucesso": False, "msg": f"Erro Banco: {str(e)}"})
    finally:
        session.close()
        
    return JSONResponse({"sucesso": True, "msg": f"{cont} itens processados!"})

@app.get("/dados-disponiveis")
async def get_dados():
    session = SessionLocal()
    anos = session.query(NFe.ano).distinct().order_by(NFe.ano).all()
    session.close()
    return {"anos": [a[0] for a in anos]}

@app.post("/processar-relatorio")
async def processar(anos: str = Form(...)):
    l_anos = anos.split(',')
    session = SessionLocal()
    try:
        res = session.query(NFe).filter(NFe.ano.in_(l_anos)).all()
        if not res: return JSONResponse({"sucesso": False, "msg": "Sem dados"})
        
        lista_dicts = [eval(row.dados_json) for row in res]
        df = pd.DataFrame(lista_dicts)
        
        # ORDEM DAS COLUNAS OBRIGATÓRIA
        cols = [
            'Mês', 'Ano', 'Chave Acesso NFe', 'Inscrição Destinatário', 'Inscrição Emitente', 
            'Razão Social Emitente', 'Cnpj Emitente', 'UF Emitente', 'Nr NFe', 'Série', 'Data NFe', 
            'BC ICMS Total', 'ICMS Total', 'BC ST Total', 'ICMS ST Total', 'Desc Total', 'IPI Total', 
            'Total Produtos', 'Total NFe', 'Descrição Produto NFe', 'NCM na NFe', 'CFOP NFe', 
            'Qtde', 'Unid', 'Vr Unit', 'Vr Total', 'Desconto Item'
        ]
        
        # Garante que as colunas existam
        for c in cols:
            if c not in df.columns: df[c] = ""
        df = df[cols]
        
        df.to_excel("Relatorio_Final.xlsx", index=False)
        
        # PROVA REAL
        soma_itens = df['Vr Total'].sum()
        df_unicas = df.drop_duplicates(subset=['Chave Acesso NFe'])
        soma_notas = df_unicas['Total NFe'].sum()
        
        return JSONResponse({
            "sucesso": True,
            "prova_real_notas": f"R$ {soma_notas:,.2f}",
            "prova_real_itens": f"R$ {soma_itens:,.2f}",
            "qtd_notas": len(df_unicas),
            "qtd_linhas": len(df),
            "download_url": "/baixar"
        })
    except Exception as e:
        return JSONResponse({"sucesso": False, "msg": str(e)})
    finally:
        session.close()

@app.get("/baixar")
async def baixar():
    return FileResponse("Relatorio_Final.xlsx", filename="Relatorio_Consolidado.xlsx")

# --- FRONTEND ---
@app.get("/", response_class=HTMLResponse)
async def home():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Extrator Fiscal</title>
        <style>
            body{font-family:sans-serif; background:#f0f2f5; padding:20px;}
            .box{background:white; padding:20px; border-radius:8px; max-width:600px; margin:0 auto 20px; box-shadow:0 2px 5px rgba(0,0,0,0.1);}
            .btn{background:#2563eb; color:white; padding:10px 20px; border:none; border-radius:4px; cursor:pointer; font-size:16px; width:100%;}
            .btn:disabled{background:#ccc;}
            .resumo{background:#dcfce7; padding:15px; border-radius:6px; margin-top:15px; display:none; border:1px solid #22c55e;}
            .val{font-size:20px; font-weight:bold; color:#15803d;}
            .drop{border:2px dashed #ccc; padding:30px; text-align:center; cursor:pointer;}
            .drop:hover{border-color:#2563eb; background:#eff6ff;}
        </style>
    </head>
    <body>
        <div class="box">
            <h2>1. Upload ZIP</h2>
            <div class="drop" onclick="document.getElementById('file').click()">
                Clique para selecionar ZIP
                <input type="file" id="file" accept=".zip" hidden onchange="upload(this.files[0])">
            </div>
            <p id="status" style="text-align:center"></p>
        </div>

        <div class="box">
            <h2>2. Relatório</h2>
            <div id="checks">Carregando anos...</div>
            <br>
            <button class="btn" id="btnGerar" onclick="gerar()" disabled>Processar</button>
            
            <div id="resumo" class="resumo">
                <h3>✅ Sucesso!</h3>
                <p>Total Notas: <span id="rNotas" class="val"></span></p>
                <p>Total Itens: <span id="rItens" class="val"></span></p>
                <a href="" id="linkDown">Baixar Arquivo</a>
            </div>
        </div>

        <script>
            async function upload(f){
                if(!f) return;
                let fd = new FormData(); fd.append('file', f);
                document.getElementById('status').innerText = "Enviando...";
                let res = await fetch('/upload', {method:'POST', body:fd});
                let d = await res.json();
                document.getElementById('status').innerText = d.msg;
                loadAnos();
            }
            async function loadAnos(){
                let res = await fetch('/dados-disponiveis');
                let d = await res.json();
                let h = '';
                d.anos.forEach(a => h += `<label style="margin-right:10px"><input type="checkbox" value="${a}" onchange="check()"> ${a}</label>`);
                document.getElementById('checks').innerHTML = h || 'Sem dados.';
            }
            function check(){
                document.getElementById('btnGerar').disabled = !document.querySelector('input:checked');
            }
            async function gerar(){
                let btn = document.getElementById('btnGerar');
                btn.disabled = true; btn.innerText = "Processando...";
                let anos = Array.from(document.querySelectorAll('input:checked')).map(x=>x.value).join(',');
                let fd = new FormData(); fd.append('anos', anos);
                
                try {
                    let res = await fetch('/processar-relatorio', {method:'POST', body:fd});
                    let d = await res.json();
                    if(d.sucesso){
                        document.getElementById('rNotas').innerText = d.prova_real_notas + " (" + d.qtd_notas + " docs)";
                        document.getElementById('rItens').innerText = d.prova_real_itens + " (" + d.qtd_linhas + " linhas)";
                        document.getElementById('linkDown').href = d.download_url;
                        document.getElementById('resumo').style.display = 'block';
                        window.location.href = d.download_url;
                    } else { alert(d.msg); }
                } catch(e){ alert("Erro ao processar"); }
                
                btn.disabled = false; btn.innerText = "Processar";
            }
            loadAnos();
        </script>
    </body>
    </html>
    """
