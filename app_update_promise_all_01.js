const axios = require('axios');
const xlsx = require('xlsx');
require('dotenv').config();

const KEY_BEARER = process.env.KEY_BEARER;
const BASE_URL = process.env.BASE_URL;
const BASE_URL_GROUPS = process.env.BASE_URL_GROUPS;

// Cache para evitar requisições duplicadas
const cache = {
    people: new Map(),
    groups: new Map()
};

// Delay helper
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

// Configurações
const CONFIG = {
    GROUP_BATCH_SIZE: 10,
    PEOPLE_BATCH_SIZE: 50,
    DELAY_BETWEEN_BATCHES: 200,
    MAX_CONCURRENT_REQUESTS: 20,
    REQUEST_TIMEOUT: 30000,
    RETRY_ATTEMPTS: 3
};

async function fetchData() {
    console.time('🚀 Tempo total de execução');
    console.log('🎯 Iniciando processamento com Promise.all()...');
    
    try {
        if (!BASE_URL_GROUPS || !KEY_BEARER) {
            throw new Error('Variáveis de ambiente não configuradas');
        }

        const axiosConfig = {
            headers: {
                'Authorization': KEY_BEARER,
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            },
            timeout: CONFIG.REQUEST_TIMEOUT
        };

        const rows = [];
        rows.push([
            'Congregacao', 'Data Cadastro', 'Nome', 'Pai', 'Mãe', 'CPF',
            'Nascimento', 'Naturalidade', 'Função', 'Batismo', 'Rua',
            'Número', 'Bairro', 'CEP', 'Contato'
        ]);

        // 1. Buscar todos os grupos
        console.log('🔍 Buscando grupos...');
        const groups = await fetchGroups(axiosConfig);
        console.log(`✅ ${groups.length} grupos encontrados`);

        // 2. Processar grupos em batches com Promise.all()
        console.log('⚡ Processando grupos em paralelo...');
        const allPeopleRows = [];
        
        for (let i = 0; i < groups.length; i += CONFIG.GROUP_BATCH_SIZE) {
            const batch = groups.slice(i, i + CONFIG.GROUP_BATCH_SIZE);
            console.log(`🔄 Processando lote ${Math.floor(i/CONFIG.GROUP_BATCH_SIZE) + 1} de ${Math.ceil(groups.length/CONFIG.GROUP_BATCH_SIZE)}`);
            
            // Processa grupos em paralelo
            const batchPromises = batch.map(group => processGroup(group, axiosConfig));
            const batchResults = await Promise.allSettled(batchPromises);
            
            // Adiciona resultados
            batchResults.forEach(result => {
                if (result.status === 'fulfilled' && result.value && result.value.rows.length > 0) {
                    allPeopleRows.push(['']); // Linha em branco entre grupos
                    allPeopleRows.push(...result.value.rows);
                }
            });
            
            // Delay entre batches para não sobrecarregar API
            if (i + CONFIG.GROUP_BATCH_SIZE < groups.length) {
                await delay(CONFIG.DELAY_BETWEEN_BATCHES);
            }
        }

        // 3. Adicionar todas as linhas
        rows.push(...allPeopleRows);

        // 4. Gerar XLSX
        console.log('📊 Gerando arquivo Excel...');
        await generateExcelFile(rows);

        console.log(`✅ Arquivo gerado com ${allPeopleRows.length} pessoas!`);
        console.timeEnd('🚀 Tempo total de execução');

    } catch (error) {
        console.error('❌ Erro:', error.message);
        return null;
    }
}

// Função para processar um grupo específico
async function processGroup(group, config) {
    try {
        const nomeCongregacao = group?.name ?? 'N/A';
        const groupRows = [];

        // Verifica cache
        const cacheKey = `group_${group.id}`;
        if (cache.groups.has(cacheKey)) {
            return cache.groups.get(cacheKey);
        }

        // Buscar membros do grupo
        const responseMembros = await axios.get(
            `${BASE_URL}/groups/${group.id}`,
            config
        );

        const membrosData = responseMembros.data.results;
        const peoples = JSON.parse(membrosData.peoples || '[]');

        if (peoples.length === 0) {
            const result = { rows: [], count: 0, groupName: nomeCongregacao };
            cache.groups.set(cacheKey, result);
            return result;
        }

        console.log(`   📁 ${nomeCongregacao}: ${peoples.length} pessoas`);

        // Processar pessoas em batches com Promise.all()
        const allPeopleData = await fetchAllPeopleData(peoples, config);

        // Transformar dados das pessoas em linhas
        allPeopleData.forEach(personData => {
            if (personData) {
                const cpf = personData.doc_1?.replace(/^(\d{3})(\d{3})(\d{3})(\d{2})$/, '$1.$2.$3-$4').slice(0, 14);
                
                groupRows.push([
                    nomeCongregacao.toUpperCase(),
                    formatarData(personData.created_at),
                    personData.full_name?.toUpperCase(),
                    personData.nomePai?.toUpperCase(),
                    personData.nomeMae?.toUpperCase(),
                    cpf,
                    formatarData(personData.birthydate),
                    personData.naturalidade?.toUpperCase(),
                    personData.funcao,
                    formatarData(personData.baptism_date),
                    personData.address_1?.toUpperCase() ?? '',
                    personData.address_number ?? '',
                    personData.address_2?.toUpperCase() ?? '',
                    personData.postal_code,
                    personData.phone_1
                ]);
            }
        });

        const result = { rows: groupRows, count: groupRows.length, groupName: nomeCongregacao };
        cache.groups.set(cacheKey, result);
        
        return result;

    } catch (error) {
        console.error(`   ❌ Erro no grupo ${group.id}:`, error.message);
        return { rows: [], count: 0, groupName: group?.name };
    }
}

// Função para buscar todas as pessoas de um grupo em batches
async function fetchAllPeopleData(personIds, config) {
    const allPeople = [];
    
    for (let i = 0; i < personIds.length; i += CONFIG.PEOPLE_BATCH_SIZE) {
        const batchIds = personIds.slice(i, i + CONFIG.PEOPLE_BATCH_SIZE);
        
        // Cria promises para o batch atual
        const batchPromises = batchIds.map(id => 
            fetchPersonWithCache(id, config)
        );
        
        // Executa todas em paralelo com Promise.allSettled()
        const batchResults = await Promise.allSettled(batchPromises);
        
        // Filtra resultados válidos
        batchResults.forEach(result => {
            if (result.status === 'fulfilled' && result.value) {
                allPeople.push(result.value);
            }
        });
        
        console.log(`   📋 Processadas ${Math.min(i + CONFIG.PEOPLE_BATCH_SIZE, personIds.length)}/${personIds.length} pessoas`);
        
        // Delay para não sobrecarregar API
        if (i + CONFIG.PEOPLE_BATCH_SIZE < personIds.length) {
            await delay(CONFIG.DELAY_BETWEEN_BATCHES);
        }
    }
    
    return allPeople;
}

// Função para buscar pessoa com cache
async function fetchPersonWithCache(personId, config) {
    const cacheKey = `person_${personId}`;
    
    if (cache.people.has(cacheKey)) {
        return cache.people.get(cacheKey);
    }

    try {
        const response = await axios.get(
            `${BASE_URL}/people/${personId}`,
            config
        );

        const peopleData = response.data.results;
        
        if (!peopleData) {
            return null;
        }

        // Processar extrafields
        const extrafields = JSON.parse(peopleData.extrafields || '[]');
        const nomePai = extrafields.find(f => f.id_ef === 15819)?.value?.trim() || '';
        const nomeMae = extrafields.find(f => f.id_ef === 15820)?.value?.trim() || '';
        const naturalidade = extrafields.find(f => f.id_ef === 15823)?.value?.trim() || '';
        const funcao = determinarFuncao(extrafields);

        const processedData = {
            ...peopleData,
            nomePai,
            nomeMae,
            naturalidade,
            funcao
        };

        cache.people.set(cacheKey, processedData);
        return processedData;

    } catch (error) {
        console.error(`      ❌ Erro pessoa ${personId}:`, error.message);
        return null;
    }
}

// Função para buscar todos os grupos
async function fetchGroups(config) {
    try {
        const response = await axios.get(BASE_URL_GROUPS, config);
        return response.data.results || [];
    } catch (error) {
        throw new Error(`Falha ao buscar grupos: ${error.message}`);
    }
}

// Função para gerar arquivo Excel
async function generateExcelFile(rows) {
    try {
        const ws = xlsx.utils.aoa_to_sheet(rows);
        
        const colWidths = [25, 20, 35, 35, 35, 15, 15, 25, 20, 15, 40, 10, 30, 12, 15];
        ws['!cols'] = colWidths.map(width => ({ wch: width }));
        
        ws['!autofilter'] = { ref: `A1:${xlsx.utils.encode_col(colWidths.length - 1)}1` };
        ws['!views'] = [{ state: 'frozen', xSplit: 0, ySplit: 1 }];
        
        const wb = xlsx.utils.book_new();
        xlsx.utils.book_append_sheet(wb, ws, 'Membros');
        
        const filename = `DADOS_ENUVENS_PROMISE_ALL_${new Date().toISOString().slice(0, 19).replace('T', ' ')}.xlsx`;
        xlsx.writeFile(wb, filename);
        
        console.log(`💾 Arquivo salvo como: ${filename}`);
        
    } catch (error) {
        throw new Error(`Falha ao gerar Excel: ${error.message}`);
    }
}

// Funções auxiliares
function formatarData(data) {
    if (!data) return "";
    
    try {
        if (data.length > 10) {
            const [ano, mes, resto] = data.split('-');
            const [dia, hora] = resto.split(' ');
            return `${dia}/${mes}/${ano} ${hora || ''}`.trim();
        } else {
            const [ano, mes, dia] = data.split("-");
            return `${dia}/${mes}/${ano}`;
        }
    } catch {
        return data;
    }
}

function determinarFuncao(extrafields) {
    const funcoes = ['MEMBRO', 'COOPERADOR', 'DIÁCONO', 'PRESBÍTERO', 'EVANGELISTA', 'PASTOR', 'CONGREGADO'];
    const campoFuncoes = extrafields.find(item => item.id_ef === "15822");
    
    if (!campoFuncoes || !campoFuncoes.sub) return 'NÃO INFORMADO';
    
    const indexAtivo = campoFuncoes.sub.findIndex(sub => sub.value === true);
    return indexAtivo !== -1 ? funcoes[indexAtivo] : 'NÃO INFORMADO';
}

// Executar
if (require.main === module) {
    fetchData().catch(console.error);
}

module.exports = { fetchData, formatarData, determinarFuncao };