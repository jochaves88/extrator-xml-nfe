import os
import shutil
import zipfile
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from typing import List
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from sqlalchemy import create_engine, Column, String, Float, or_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

app = FastAPI()

# --- CONFIGURAÇÃO DE PASTAS ---
TEMP_DIR = "temp_files"
REPORTS_DIR = "meus_relatorios"

for d in [TEMP_DIR, REPORTS_DIR]:
    if not os.path.exists(d): os.makedirs(d)

# --- BANCO DE DADOS ---
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./nfe_data.db")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class NFe(Base):
    __tablename__ = "notas_fiscais_v5" # V5: Nova estrutura
    chave_item = Column(String, primary_key=True, index=True)
    chave_acesso = Column(String)
    ano = Column(String)
    mes = Column(String)
    cnpj_emitente = Column(String)     # Para filtros rápidos
    cnpj_destinatario = Column(String) # Para filtros rápidos
    dados_json = Column(String)

Base.metadata.create_all(bind=engine)

# --- FUNÇÕES ---
ns_map = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

def get_val(node, path, type_fn=str):
    if node is None: return type_fn(0) if type_fn in [float, int] else ""
    try:
        r = node.find(path, ns_map)
        if r is None: r = node.find(path.replace('nfe:', ''))
        if r is not None and r.text:
            return type_fn(r.text.replace(',', '.'))
    except: pass
    return type_fn(0) if type_fn in [float, int] else ""

def parse_xml(filepath):
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()
        if 'nfeProc' in root.tag: inf = root.find('.//nfe:infNFe', ns_map)
        else: inf = root.find('nfe:infNFe', ns_map)
        if inf is None: return []

        ide = inf.find('nfe:ide', ns_map)
        emit = inf.find('nfe:emit', ns_map)
        dest = inf.find('nfe:dest', ns_map)
        total = inf.find('.//nfe:ICMSTot', ns_map)
        
        prot = root.find('.//nfe:infProt', ns_map)
        chave = get_val(prot, 'nfe:chNFe') or inf.attrib.get('Id', '')[3:]
        
        dt_str = get_val(ide, 'nfe:dhEmi') or get_val(ide, 'nfe:dEmi')
        dt = datetime.now()
        if len(dt_str) >= 10: dt = datetime.strptime(dt_str[:10], '%Y-%m-%d')

        totais = {
            'vNF': get_val(total, 'nfe:vNF', float),
            'vProd': get_val(total, 'nfe:vProd', float),
            'vFrete': get_val(total, 'nfe:vFrete', float),
            'vSeg': get_val(total, 'nfe:vSeg', float),
            'vDesc': get_val(total, 'nfe:vDesc', float),
            'vOutro': get_val(total, 'nfe:vOutro', float),
            'vIPI': get_val(total, 'nfe:vIPI', float),
            'vPIS': get_val(total, 'nfe:vPIS', float),
            'vCOFINS': get_val(total, 'nfe:vCOFINS', float),
        }

        itens_db = []
        for i, det in enumerate(inf.findall('nfe:det', ns_map)):
            prod = det.find('nfe:prod', ns_map)
            imposto = det.find('nfe:imposto', ns_map)
            
            row = {
                'Mês': str(dt.month).zfill(2),
                'Ano': str(dt.year),
                'Chave Acesso': chave,
                'Número NFe': get_val(ide, 'nfe:nNF'),
                'Série': get_val(ide, 'nfe:serie'),
                'Data Emissão': dt.strftime('%d/%m/%Y'),
                'Nat. Operação': get_val(ide, 'nfe:natOp'),
                'Emitente Nome': get_val(emit, 'nfe:xNome'),
                'Emitente CNPJ': get_val(emit, 'nfe:CNPJ'),
                'Destinatário Nome': get_val(dest, 'nfe:xNome'),
                'Destinatário CNPJ': get_val(dest, 'nfe:CNPJ'),
                'UF': get_val(emit, 'nfe:enderEmit/nfe:UF'),
                'Vl Total NFe': totais['vNF'],
                'Vl Produtos (Nota)': totais['vProd'],
                'Vl Frete (Nota)': totais['vFrete'],
                'Vl Seguro (Nota)': totais['vSeg'],
                'Vl Desconto (Nota)': totais['vDesc'],
                'Vl Outras (Nota)': totais['vOutro'],
                'Vl IPI (Nota)': totais['vIPI'],
                'Vl PIS (Nota)': totais['vPIS'],
                'Vl COFINS (Nota)': totais['vCOFINS'],
                'Cód. Prod': get_val(prod, 'nfe:cProd'),
                'Descrição Produto': get_val(prod, 'nfe:xProd'),
                'NCM': get_val(prod, 'nfe:NCM'),
                'CEST': get_val(prod, 'nfe:CEST'),
                'CFOP': get_val(prod, 'nfe:CFOP'),
                'Unidade': get_val(prod, 'nfe:uCom'),
                'Qtde': get_val(prod, 'nfe:qCom', float),
                'Vl Unitário': get_val(prod, 'nfe:vUnCom', float),
                'Vl Total Item': get_val(prod, 'nfe:vProd', float),
                'ICMS Item': get_val(imposto, './/nfe:ICMS//nfe:vICMS', float),
                'IPI Item': get_val(imposto, './/nfe:IPI//nfe:vIPI', float),
                'PIS Item': get_val(imposto, './/nfe:PIS//nfe:vPIS', float),
                'COFINS Item': get_val(imposto, './/nfe:COFINS//nfe:vCOFINS', float),
            }
            
            itens_db.append(NFe(
                chave_item=f"{chave}-{i+1}", 
                chave_acesso=chave, 
                ano=str(dt.year),
                mes=str(dt.month).zfill(2),
                cnpj_emitente=get_val(emit, 'nfe:CNPJ'),
                cnpj_destinatario=get_val(dest, 'nfe:CNPJ'),
                dados_json=str(row)
            ))
        return itens_db
    except: return []

# --- ROTAS ---
@app.post("/upload")
async def upload(files: List[UploadFile] = File(...)):
    # Limpa temp, mas mantem os dados no banco
    if os.path.exists(TEMP_DIR): 
        shutil.rmtree(TEMP_DIR)
        os.makedirs(TEMP_DIR)

    c_arquivos = 0
    c_itens = 0
    sess = SessionLocal()

    try:
        for file in files:
            file_path = os.path.join(TEMP_DIR, file.filename)
            with open(file_path, "wb") as b: 
                shutil.copyfileobj(file.file, b)
            
            # Extrai ZIP
            try:
                with zipfile.ZipFile(file_path, 'r') as z: 
                    z.extractall(TEMP_DIR)
            except: 
                continue # Pula se não for zip válido

        # Processa XMLs extraídos
        xml_files = glob.glob(f"{TEMP_DIR}/**/*.xml", recursive=True) + glob.glob(f"{TEMP_DIR}/**/*.XML", recursive=True)
        c_arquivos = len(xml_files)

        for f in xml_files:
            itens = parse_xml(f)
            for item in itens:
                sess.merge(item) # Upsert
                c_itens += 1
        sess.commit()

        return JSONResponse({
            "ok": True, 
            "msg": f"{c_arquivos} XMLs lidos. {c_itens} itens salvos/atualizados."
        })

    except Exception as e:
        sess.rollback()
        return JSONResponse({"ok": False, "msg": str(e)})
    finally:
        sess.close()

@app.get("/filtros")
async def get_filtros():
    s = SessionLocal()
    anos = s.query(NFe.ano).distinct().order_by(NFe.ano).all()
    s.close()
    return {"anos": [x[0] for x in anos]}

@app.get("/historico")
async def get_historico():
    # Lista arquivos na pasta de relatórios
    files = []
    for f in os.listdir(REPORTS_DIR):
        if f.endswith(".xlsx"):
            path = os.path.join(REPORTS_DIR, f)
            t = os.path.getmtime(path)
            dt = datetime.fromtimestamp(t).strftime('%d/%m/%Y %H:%M')
            files.append({"nome": f, "data": dt})
    # Ordena do mais recente para o mais antigo
    files.sort(key=lambda x: x['nome'], reverse=True)
    return {"arquivos": files}

@app.post("/gerar")
async def gerar(anos: str = Form(...), meu_cnpj: str = Form("")):
    s = SessionLocal()
    try:
        l_anos = anos.split(',')
        res = s.query(NFe).filter(NFe.ano.in_(l_anos)).all()
        
        if not res: return JSONResponse({"ok": False, "msg": "Sem dados."})

        data = [eval(r.dados_json) for r in res]
        df = pd.DataFrame(data)

        # --- LÓGICA DE CLASSIFICAÇÃO (Entrada/Saída) ---
        def classificar(row):
            if not meu_cnpj: return "Indefinido"
            # Limpa pontuação para comparar
            cnpj_limpo = ''.join(filter(str.isdigit, meu_cnpj))
            emit = ''.join(filter(str.isdigit, str(row.get('Emitente CNPJ', ''))))
            dest = ''.join(filter(str.isdigit, str(row.get('Destinatário CNPJ', ''))))
            
            if emit == cnpj_limpo: return "SAÍDA (Venda)"
            if dest == cnpj_limpo: return "ENTRADA (Compra)"
            return "Terceiros"

        df['Tipo Operação'] = df.apply(classificar, axis=1)

        # Ordenação
        df = df.sort_values(by=['Ano', 'Mês', 'Data Emissão'])

        # Colunas Finais (Adicionando a nova coluna Tipo Operação no início)
        cols = [
            'Tipo Operação', 'Mês', 'Ano', 'Data Emissão', 'Número NFe', 'Série', 'Chave Acesso',
            'Emitente Nome', 'Emitente CNPJ', 'Destinatário Nome', 'Destinatário CNPJ', 'UF', 'Nat. Operação',
            'Cód. Prod', 'Descrição Produto', 'NCM', 'CEST', 'CFOP', 'Unidade', 'Qtde', 'Vl Unitário', 'Vl Total Item',
            'Vl Total NFe', 'Vl Produtos (Nota)', 'Vl Frete (Nota)', 'Vl Seguro (Nota)', 'Vl Desconto (Nota)', 
            'Vl Outras (Nota)', 'Vl IPI (Nota)', 'Vl PIS (Nota)', 'Vl COFINS (Nota)',
            'ICMS Item', 'IPI Item', 'PIS Item', 'COFINS Item'
        ]
        
        df = df.reindex(columns=cols).fillna("")

        # Nome Único
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"Relatorio_NFe_{timestamp}.xlsx"
        filepath = os.path.join(REPORTS_DIR, filename)
        
        df.to_excel(filepath, index=False)

        # Prova Real
        v_itens = df['Vl Total Item'].replace('', 0).astype(float).sum()
        df_unicas = df.drop_duplicates(subset=['Chave Acesso'])
        v_notas = df_unicas['Vl Total NFe'].replace('', 0).astype(float).sum()
        
        # Filtros para resumo
        entradas = len(df_unicas[df_unicas['Tipo Operação'].str.contains('ENTRADA')]) if meu_cnpj else 0
        saidas = len(df_unicas[df_unicas['Tipo Operação'].str.contains('SAÍDA')]) if meu_cnpj else 0

        return JSONResponse({
            "ok": True,
            "filename": filename,
            "notas": f"R$ {v_notas:,.2f}",
            "itens": f"R$ {v_itens:,.2f}",
            "qtd_notas": len(df_unicas),
            "resumo_ops": f"Entradas: {entradas} | Saídas: {saidas}" if meu_cnpj else "Sem CNPJ base definido",
            "url": f"/download/{filename}"
        })
    finally: s.close()

@app.get("/download/{filename}")
async def download(filename: str):
    path = os.path.join(REPORTS_DIR, filename)
    if os.path.exists(path):
        return FileResponse(path, filename=filename)
    return JSONResponse({"msg": "Arquivo não encontrado"}, 404)

# --- FRONTEND (HTML/JS) ---
@app.get("/", response_class=HTMLResponse)
async def home():
    return """
    <!DOCTYPE html>
    <html lang="pt-br">
    <head>
        <meta charset="UTF-8">
        <title>Extrator Fiscal Ultimate</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <script src="https://cdn.jsdelivr.net/npm/sweetalert2@11"></script>
        <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
    </head>
    <body class="bg-slate-50 min-h-screen text-slate-800 font-sans">
        
        <div class="max-w-6xl mx-auto p-6 grid grid-cols-1 lg:grid-cols-3 gap-6">
            
            <div class="lg:col-span-2 space-y-6">
                <div class="bg-white p-6 rounded-xl shadow border border-slate-200">
                    <h2 class="text-xl font-bold text-blue-700 mb-4"><i class="fas fa-upload"></i> Importar Dados</h2>
                    
                    <div id="dropZone" class="border-2 border-dashed border-slate-300 rounded-lg p-10 text-center hover:bg-blue-50 hover:border-blue-400 transition cursor-pointer group">
                        <i class="fas fa-file-archive text-5xl text-slate-300 group-hover:text-blue-500 mb-3 transition"></i>
                        <p class="font-medium text-slate-600">Arraste múltiplos ZIPs aqui</p>
                        <p class="text-xs text-slate-400">O sistema acumula os dados, não apaga os anteriores.</p>
                        <input type="file" id="fileInput" accept=".zip" multiple class="hidden">
                    </div>
                </div>

                <div class="bg-white p-6 rounded-xl shadow border border-slate-200">
                    <h2 class="text-xl font-bold text-green-700 mb-4"><i class="fas fa-filter"></i> Gerar Relatório</h2>
                    
                    <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
                        <div>
                            <label class="block text-sm font-bold text-slate-600 mb-1">Seu CNPJ (Opcional)</label>
                            <input type="text" id="meuCnpj" placeholder="Apenas números" class="w-full border border-slate-300 rounded p-2 focus:ring focus:ring-blue-200 outline-none">
                            <p class="text-xs text-slate-400 mt-1">Usado para separar Entrada vs Saída.</p>
                        </div>
                        <div>
                            <label class="block text-sm font-bold text-slate-600 mb-1">Filtro de Anos</label>
                            <div id="anosList" class="flex flex-wrap gap-2 text-sm">
                                Carregando...
                            </div>
                        </div>
                    </div>

                    <button id="btnGerar" onclick="gerar()" disabled class="w-full bg-slate-300 text-white font-bold py-3 rounded-lg shadow transition">
                        Selecione pelo menos um ano
                    </button>
                </div>
            </div>

            <div class="bg-white p-6 rounded-xl shadow border border-slate-200 h-fit">
                <h2 class="text-xl font-bold text-purple-700 mb-4"><i class="fas fa-history"></i> Meus Relatórios</h2>
                <p class="text-sm text-slate-500 mb-4">Baixe arquivos gerados anteriormente.</p>
                
                <div id="historicoList" class="space-y-2 max-h-[500px] overflow-y-auto pr-2">
                    <div class="text-center text-slate-400 py-4"><i class="fas fa-circle-notch fa-spin"></i></div>
                </div>
                
                <button onclick="loadHistorico()" class="mt-4 text-sm text-blue-600 hover:underline w-full text-center">
                    <i class="fas fa-sync"></i> Atualizar Lista
                </button>
            </div>

        </div>

        <script>
            // --- UPLOAD MULTIPLO ---
            const dropZone = document.getElementById('dropZone');
            const fileInput = document.getElementById('fileInput');

            dropZone.onclick = () => fileInput.click();
            fileInput.onchange = () => handleFiles(fileInput.files);
            
            ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(evt => {
                dropZone.addEventListener(evt, e => { e.preventDefault(); e.stopPropagation(); });
            });
            dropZone.addEventListener('drop', e => handleFiles(e.dataTransfer.files));

            async function handleFiles(files) {
                if(!files.length) return;
                
                let fd = new FormData();
                let hasZip = false;
                for(let i=0; i<files.length; i++) {
                    if(files[i].name.toLowerCase().endsWith('.zip')) {
                        fd.append('files', files[i]);
                        hasZip = true;
                    }
                }

                if(!hasZip) return Swal.fire('Erro', 'Envie apenas arquivos ZIP.', 'error');

                Swal.fire({
                    title: 'Importando...', 
                    html: 'Processando arquivos e salvando no banco.<br>Isso pode levar um tempo.', 
                    didOpen: () => Swal.showLoading()
                });

                try {
                    let res = await fetch('/upload', {method:'POST', body:fd});
                    let data = await res.json();
                    
                    if(data.ok) {
                        Swal.fire('Sucesso!', data.msg, 'success');
                        loadFiltros();
                    } else {
                        Swal.fire('Erro', data.msg, 'error');
                    }
                } catch(e) { Swal.fire('Erro', 'Falha na conexão.', 'error'); }
            }

            // --- FILTROS E HISTORICO ---
            async function loadFiltros() {
                let res = await fetch('/filtros');
                let data = await res.json();
                let div = document.getElementById('anosList');
                div.innerHTML = '';
                
                if(data.anos.length === 0) div.innerHTML = 'Nenhum dado.';

                data.anos.forEach(ano => {
                    div.innerHTML += `
                        <label class="cursor-pointer select-none">
                            <input type="checkbox" value="${ano}" onchange="checkBtn()" class="peer sr-only">
                            <span class="px-3 py-1 rounded bg-slate-100 border text-slate-500 peer-checked:bg-green-600 peer-checked:text-white peer-checked:border-green-600 transition text-xs font-bold">
                                ${ano}
                            </span>
                        </label>`;
                });
            }

            async function loadHistorico() {
                let div = document.getElementById('historicoList');
                try {
                    let res = await fetch('/historico');
                    let data = await res.json();
                    
                    div.innerHTML = '';
                    if(data.arquivos.length === 0) {
                        div.innerHTML = '<div class="text-sm text-slate-400 text-center">Nenhum relatório gerado.</div>';
                        return;
                    }

                    data.arquivos.forEach(file => {
                        div.innerHTML += `
                            <div class="flex items-center justify-between p-3 bg-slate-50 rounded hover:bg-blue-50 border border-slate-100 transition group">
                                <div>
                                    <div class="text-sm font-bold text-slate-700 truncate w-40" title="${file.nome}">${file.nome}</div>
                                    <div class="text-xs text-slate-400">${file.data}</div>
                                </div>
                                <a href="/download/${file.nome}" class="text-blue-500 hover:text-blue-700 bg-white p-2 rounded-full shadow-sm">
                                    <i class="fas fa-download"></i>
                                </a>
                            </div>
                        `;
                    });
                } catch(e) { div.innerHTML = 'Erro ao carregar.'; }
            }

            function checkBtn() {
                let count = document.querySelectorAll('input[type="checkbox"]:checked').length;
                let btn = document.getElementById('btnGerar');
                if(count > 0) {
                    btn.disabled = false;
                    btn.className = "w-full bg-green-600 hover:bg-green-700 text-white font-bold py-3 rounded-lg shadow transition transform active:scale-95";
                    btn.innerText = "Gerar Novo Relatório";
                } else {
                    btn.disabled = true;
                    btn.className = "w-full bg-slate-300 text-white font-bold py-3 rounded-lg shadow transition cursor-not-allowed";
                    btn.innerText = "Selecione pelo menos um ano";
                }
            }

            async function gerar() {
                let anos = Array.from(document.querySelectorAll('input[type="checkbox"]:checked')).map(x => x.value).join(',');
                let cnpj = document.getElementById('meuCnpj').value;
                
                Swal.fire({title: 'Gerando Relatório...', html: 'Aplicando filtros e ordenando...', didOpen: () => Swal.showLoading()});

                let fd = new FormData();
                fd.append('anos', anos);
                fd.append('meu_cnpj', cnpj);

                try {
                    let res = await fetch('/gerar', {method:'POST', body:fd});
                    let data = await res.json();

                    if(data.ok) {
                        loadHistorico(); // Atualiza a lista lateral
                        
                        Swal.fire({
                            title: 'Relatório Pronto!',
                            icon: 'success',
                            html: `
                                <div class="bg-slate-100 p-3 rounded text-left text-sm space-y-2 mb-4">
                                    <div class="flex justify-between border-b pb-1"><span>Total Notas:</span> <span class="font-bold text-green-700">${data.notas}</span></div>
                                    <div class="flex justify-between border-b pb-1"><span>Total Itens:</span> <span class="font-bold text-blue-700">${data.itens}</span></div>
                                    <div class="flex justify-between border-b pb-1"><span>Qtd Notas:</span> <span>${data.qtd_notas}</span></div>
                                    <div class="text-center font-bold text-slate-600 pt-1">${data.resumo_ops}</div>
                                </div>
                            `,
                            confirmButtonText: 'Baixar Agora',
                            showCancelButton: true,
                            cancelButtonText: 'Fechar'
                        }).then((result) => {
                            if(result.isConfirmed) window.location.href = data.url;
                        });
                    } else { Swal.fire('Erro', data.msg, 'error'); }
                } catch(e) { Swal.fire('Erro', 'Falha ao processar.', 'error'); }
            }

            // Inicialização
            loadFiltros();
            loadHistorico();
        </script>
    </body>
    </html>
    """
