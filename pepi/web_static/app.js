// Global state
let currentFileId = null;
let charts = {};
let currentSort = { table: null, column: null, direction: 'asc' };

// Initialize the application
document.addEventListener('DOMContentLoaded', function() {
    initializeEventListeners();
    loadUploadedFiles();
});

function initializeEventListeners() {
    // File upload
    const fileInput = document.getElementById('fileInput');
    const uploadArea = document.getElementById('uploadArea');
    
    fileInput.addEventListener('change', handleFileUpload);
    
    // Drag and drop
    uploadArea.addEventListener('dragover', handleDragOver);
    uploadArea.addEventListener('dragleave', handleDragLeave);
    uploadArea.addEventListener('drop', handleFileDrop);
    
    // Tab switching
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => switchTab(btn.dataset.tab));
    });
}

// File upload handling
function handleFileUpload(event) {
    const file = event.target.files[0];
    if (file) {
        uploadFile(file);
    }
}

function handleDragOver(event) {
    event.preventDefault();
    event.currentTarget.classList.add('dragover');
}

function handleDragLeave(event) {
    event.preventDefault();
    event.currentTarget.classList.remove('dragover');
}

function handleFileDrop(event) {
    event.preventDefault();
    event.currentTarget.classList.remove('dragover');
    
    const files = event.dataTransfer.files;
    if (files.length > 0) {
        uploadFile(files[0]);
    }
}

async function uploadFile(file) {
    const formData = new FormData();
    formData.append('file', file);
    
    showProgress(0, 'Uploading file...');
    
    try {
        const response = await fetch('/api/upload', {
            method: 'POST',
            body: formData
        });
        
        if (!response.ok) {
            throw new Error(`Upload failed: ${response.statusText}`);
        }
        
        const result = await response.json();
        showProgress(100, 'Upload complete!');
        
        setTimeout(() => {
            hideProgress();
            showToast('success', `File uploaded successfully: ${result.filename}`);
            loadUploadedFiles();
        }, 1000);
        
    } catch (error) {
        hideProgress();
        showToast('error', `Upload failed: ${error.message}`);
    }
}

function showProgress(percent, text) {
    const container = document.getElementById('progressContainer');
    const fill = document.getElementById('progressFill');
    const textEl = document.getElementById('progressText');
    
    container.style.display = 'block';
    fill.style.width = percent + '%';
    textEl.textContent = text;
}

function hideProgress() {
    document.getElementById('progressContainer').style.display = 'none';
}

// Load and display uploaded files
async function loadUploadedFiles() {
    try {
        const response = await fetch('/api/files');
        const data = await response.json();
        
        const filesSection = document.getElementById('filesSection');
        const filesList = document.getElementById('filesList');
        
        if (data.files.length > 0) {
            filesSection.style.display = 'block';
            filesList.innerHTML = '';
            
            let preloadedFile = null;
            
            data.files.forEach(file => {
                const fileItem = createFileItem(file);
                filesList.appendChild(fileItem);
                
                // Check if this is a pre-loaded file (first file loaded)
                if (!currentFileId && !preloadedFile) {
                    preloadedFile = file;
                }
            });
            
            // Auto-select the first file if no file is currently selected
            if (preloadedFile && !currentFileId) {
                setTimeout(() => {
                    selectFile(preloadedFile.file_id);
                    showToast('success', `Auto-loaded: ${preloadedFile.filename}`);
                }, 500);
            }
        } else {
            filesSection.style.display = 'none';
        }
    } catch (error) {
        showToast('error', 'Failed to load files');
    }
}

function createFileItem(file) {
    const div = document.createElement('div');
    div.className = 'file-item';
    div.dataset.fileId = file.file_id;
    
    div.innerHTML = `
        <div class="file-info">
            <h4>${file.filename}</h4>
            <p>${formatFileSize(file.size)} • ${file.lines.toLocaleString()} lines</p>
        </div>
        <div class="file-actions">
            <button class="btn btn-primary" onclick="selectFile('${file.file_id}')">
                <i class="fas fa-eye"></i> Analyze
            </button>
            <button class="btn btn-secondary" onclick="downloadFile('${file.file_id}')">
                <i class="fas fa-download"></i> Download
            </button>
            <button class="btn btn-danger" onclick="deleteFile('${file.file_id}')">
                <i class="fas fa-trash"></i> Delete
            </button>
        </div>
    `;
    
    return div;
}

function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

// File selection and analysis
async function selectFile(fileId) {
    currentFileId = fileId;
    
    // Update UI
    document.querySelectorAll('.file-item').forEach(item => {
        item.classList.remove('selected');
    });
    document.querySelector(`[data-file-id="${fileId}"]`).classList.add('selected');
    
    // Show dashboard
    document.getElementById('dashboard').style.display = 'block';
    
    // Load basic info
    await analyzeBasicInfo();
}

async function downloadFile(fileId) {
    try {
        const response = await fetch(`/api/download/${fileId}`);
        if (!response.ok) throw new Error('Download failed');
        
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'log_file.log';
        a.click();
        window.URL.revokeObjectURL(url);
    } catch (error) {
        showToast('error', 'Download failed');
    }
}

async function deleteFile(fileId) {
    if (!confirm('Are you sure you want to delete this file?')) return;
    
    try {
        const response = await fetch(`/api/files/${fileId}`, { method: 'DELETE' });
        if (!response.ok) throw new Error('Delete failed');
        
        showToast('success', 'File deleted successfully');
        loadUploadedFiles();
        
        if (currentFileId === fileId) {
            currentFileId = null;
            document.getElementById('dashboard').style.display = 'none';
        }
    } catch (error) {
        showToast('error', 'Delete failed');
    }
}

// Tab switching
function switchTab(tabName) {
    // Update tab buttons
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    document.querySelector(`[data-tab="${tabName}"]`).classList.add('active');
    
    // Update tab content
    document.querySelectorAll('.tab-pane').forEach(pane => {
        pane.classList.remove('active');
    });
    document.getElementById(tabName).classList.add('active');
}

// Analysis functions
async function analyzeBasicInfo() {
    if (!currentFileId) return;
    
    showLoading('Analyzing basic information...');
    
    try {
        const response = await fetch(`/api/analyze/${currentFileId}/basic`, { method: 'POST' });
        const result = await response.json();
        
        if (result.status === 'success') {
            displayBasicInfo(result.data);
        } else {
            showToast('error', 'Basic analysis failed');
        }
    } catch (error) {
        showToast('error', `Analysis failed: ${error.message}`);
    } finally {
        hideLoading();
    }
}

function displayBasicInfo(data) {
    const fileInfo = document.getElementById('fileInfo');
    const mongoInfo = document.getElementById('mongoInfo');
    
    fileInfo.innerHTML = `
        <p><strong>Filename:</strong> ${data.filename}</p>
        <p><strong>Size:</strong> ${formatFileSize(data.size)}</p>
        <p><strong>Lines:</strong> ${data.lines.toLocaleString()}</p>
        <p><strong>Start Date:</strong> ${data.start_date || 'N/A'}</p>
        <p><strong>End Date:</strong> ${data.end_date || 'N/A'}</p>
    `;
    
    const startupOptionsHtml = data.startup_options ? 
        `<div class="json-viewer">
            <div class="json-copy-btn" onclick="copyJsonToClipboard('startupOptionsJson')">
                <i class="fas fa-copy"></i> Copy
            </div>
            <pre id="startupOptionsJson">${syntaxHighlightJson(data.startup_options)}</pre>
        </div>` : 
        '<p><em>No startup options found</em></p>';
    
    mongoInfo.innerHTML = `
        <p><strong>MongoDB Version:</strong> ${data.db_version || 'N/A'}</p>
        <p><strong>OS Version:</strong> ${data.os_version || 'N/A'}</p>
        <p><strong>Kernel Version:</strong> ${data.kernel_version || 'N/A'}</p>
        <div style="margin-top: 15px;">
            <strong>Startup Configuration:</strong>
            ${startupOptionsHtml}
        </div>
    `;
}

async function analyzeConnections() {
    if (!currentFileId) return;
    
    showLoading('Analyzing connections...');
    
    try {
        const response = await fetch(`/api/analyze/${currentFileId}/connections`, { method: 'POST' });
        const result = await response.json();
        
        if (result.status === 'success') {
            displayConnectionsData(result.data);
        } else {
            showToast('error', 'Connection analysis failed');
        }
    } catch (error) {
        showToast('error', `Analysis failed: ${error.message}`);
    } finally {
        hideLoading();
    }
}

function displayConnectionsData(data) {
    const statsGrid = document.getElementById('connectionStats');
    const tableContainer = document.getElementById('connectionsTable');
    
    // Display stats
    statsGrid.innerHTML = `
        <div class="stat-card">
            <h3>${data.total_opened.toLocaleString()}</h3>
            <p>Connections Opened</p>
        </div>
        <div class="stat-card">
            <h3>${data.total_closed.toLocaleString()}</h3>
            <p>Connections Closed</p>
        </div>
        <div class="stat-card">
            <h3>${Object.keys(data.connections).length}</h3>
            <p>Unique IPs</p>
        </div>
        ${data.overall_stats ? `
        <div class="stat-card">
            <h3>${data.overall_stats.avg.toFixed(1)}s</h3>
            <p>Avg Duration</p>
        </div>` : ''}
    `;
    
    // Create connections chart
    createConnectionsChart(data.connections);
    
    // Create connections table
    const table = document.createElement('table');
    table.className = 'data-table';
    
    const headers = ['IP Address', 'Opened', 'Closed', 'Balance'];
    if (data.overall_stats) {
        headers.push('Avg Duration', 'Min Duration', 'Max Duration');
    }
    
    table.innerHTML = `
        <thead>
            <tr>${headers.map(h => `<th>${h}</th>`).join('')}</tr>
        </thead>
        <tbody>
            ${Object.entries(data.connections).map(([ip, conn]) => {
                let row = `
                    <tr>
                        <td>${ip}</td>
                        <td>${conn.opened}</td>
                        <td>${conn.closed}</td>
                        <td>${conn.opened - conn.closed}</td>
                `;
                
                if (data.ip_stats && data.ip_stats[ip]) {
                    const stats = data.ip_stats[ip];
                    row += `
                        <td>${stats.avg.toFixed(1)}s</td>
                        <td>${stats.min.toFixed(1)}s</td>
                        <td>${stats.max.toFixed(1)}s</td>
                    `;
                }
                
                row += '</tr>';
                return row;
            }).join('')}
        </tbody>
    `;
    
    tableContainer.innerHTML = '';
    tableContainer.appendChild(table);
    
    // Make table sortable
    makeSortable(table, 'connections');
}

function createConnectionsChart(connections) {
    const ctx = document.getElementById('connectionsChart').getContext('2d');
    
    // Destroy existing chart if it exists
    if (charts.connections) {
        charts.connections.destroy();
    }
    
    const ips = Object.keys(connections).slice(0, 10); // Top 10 IPs
    const opened = ips.map(ip => connections[ip].opened);
    const closed = ips.map(ip => connections[ip].closed);
    
    charts.connections = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ips,
            datasets: [
                {
                    label: 'Opened',
                    data: opened,
                    backgroundColor: 'rgba(102, 126, 234, 0.7)',
                    borderColor: 'rgba(102, 126, 234, 1)',
                    borderWidth: 1
                },
                {
                    label: 'Closed',
                    data: closed,
                    backgroundColor: 'rgba(118, 75, 162, 0.7)',
                    borderColor: 'rgba(118, 75, 162, 1)',
                    borderWidth: 1
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: 'Connections by IP Address (Top 10)'
                }
            },
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        }
    });
}

async function analyzeQueries() {
    if (!currentFileId) return;
    
    const namespace = document.getElementById('namespaceFilter').value;
    const operation = document.getElementById('operationFilter').value;
    
    showLoading('Analyzing queries...');
    
    try {
        let url = `/api/analyze/${currentFileId}/queries`;
        const params = new URLSearchParams();
        if (namespace) params.append('namespace', namespace);
        if (operation) params.append('operation', operation);
        if (params.toString()) url += '?' + params.toString();
        
        const response = await fetch(url, { method: 'POST' });
        const result = await response.json();
        
        if (result.status === 'success') {
            displayQueriesData(result.data);
        } else {
            showToast('error', 'Query analysis failed');
        }
    } catch (error) {
        showToast('error', `Analysis failed: ${error.message}`);
    } finally {
        hideLoading();
    }
}

// Global storage for queries data (for context menu)
let currentQueriesData = null;
let selectedQueryIndex = null;

function displayQueriesData(data) {
    // Store queries data globally
    currentQueriesData = data;
    
    const statsGrid = document.getElementById('queryStats');
    const tableContainer = document.getElementById('queriesTable');
    
    // Calculate stats
    const totalQueries = data.queries.reduce((sum, q) => sum + q.count, 0);
    const avgExecutionTime = data.queries.reduce((sum, q) => sum + q.mean_ms, 0) / data.queries.length;
    const slowestQuery = Math.max(...data.queries.map(q => q.max_ms));
    const collscans = data.queries.filter(q => q.indexes && q.indexes.includes('COLLSCAN')).length;
    
    // Display stats
    statsGrid.innerHTML = `
        <div class="stat-card">
            <h3>${data.total_patterns}</h3>
            <p>Query Patterns</p>
        </div>
        <div class="stat-card">
            <h3>${totalQueries.toLocaleString()}</h3>
            <p>Total Queries</p>
        </div>
        <div class="stat-card">
            <h3>${avgExecutionTime.toFixed(1)}ms</h3>
            <p>Avg Execution Time</p>
        </div>
        <div class="stat-card">
            <h3>${slowestQuery.toFixed(1)}ms</h3>
            <p>Slowest Query</p>
        </div>
        <div class="stat-card">
            <h3>${collscans}</h3>
            <p>Collection Scans</p>
        </div>
    `;
    
    // Create queries chart
    createQueriesChart(data.queries);
    
    // Create queries table
    const table = document.createElement('table');
    table.className = 'data-table';
    
    table.innerHTML = `
        <thead>
            <tr>
                <th>Namespace</th>
                <th>Operation</th>
                <th>Pattern</th>
                <th>Count</th>
                <th>Min (ms)</th>
                <th>Max (ms)</th>
                <th>Mean (ms)</th>
                <th>95% (ms)</th>
                <th>Index</th>
            </tr>
        </thead>
        <tbody>
            ${data.queries.map((query, index) => `
                <tr data-query-index="${index}">
                    <td>${query.namespace}</td>
                    <td>${query.operation}</td>
                    <td>
                        <span class="pattern-text pattern-clickable" 
                              title="Click to view query examples" 
                              data-namespace="${query.namespace}" 
                              data-operation="${query.operation}" 
                              data-pattern="${encodeURIComponent(query.pattern)}" 
                              data-row-index="${index}"
                              onclick="showQueryExamplesFromElement(this)">
                            ${truncateText(query.pattern, 50)}
                        </span>
                    </td>
                    <td>${query.count}</td>
                    <td>${query.min_ms.toFixed(1)}</td>
                    <td>${query.max_ms.toFixed(1)}</td>
                    <td>${query.mean_ms.toFixed(1)}</td>
                    <td>${query.percentile_95_ms.toFixed(1)}</td>
                    <td>${formatIndexes(query.indexes)}</td>
                </tr>
            `).join('')}
        </tbody>
    `;
    
    tableContainer.innerHTML = '';
    tableContainer.appendChild(table);
    
    // Make table sortable
    makeSortable(table, 'queries');
}

function createQueriesChart(queries) {
    const ctx = document.getElementById('queriesChart').getContext('2d');
    
    // Destroy existing chart if it exists
    if (charts.queries) {
        charts.queries.destroy();
    }
    
    // Get top 10 queries by count
    const topQueries = queries
        .sort((a, b) => b.count - a.count)
        .slice(0, 10);
    
    const labels = topQueries.map(q => `${q.namespace}.${q.operation}`);
    const counts = topQueries.map(q => q.count);
    const avgTimes = topQueries.map(q => q.mean_ms);
    
    charts.queries = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Query Count',
                    data: counts,
                    backgroundColor: 'rgba(102, 126, 234, 0.7)',
                    borderColor: 'rgba(102, 126, 234, 1)',
                    borderWidth: 1,
                    yAxisID: 'y'
                },
                {
                    label: 'Avg Time (ms)',
                    data: avgTimes,
                    backgroundColor: 'rgba(118, 75, 162, 0.7)',
                    borderColor: 'rgba(118, 75, 162, 1)',
                    borderWidth: 1,
                    yAxisID: 'y1'
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: 'Top 10 Query Patterns'
                }
            },
            scales: {
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left',
                    title: {
                        display: true,
                        text: 'Query Count'
                    }
                },
                y1: {
                    type: 'linear',
                    display: true,
                    position: 'right',
                    title: {
                        display: true,
                        text: 'Avg Time (ms)'
                    },
                    grid: {
                        drawOnChartArea: false,
                    },
                }
            }
        }
    });
}

function formatIndexes(indexes) {
    if (!indexes || indexes.length === 0) {
        return '<span class="index-badge">N/A</span>';
    }
    
    return indexes.map(index => {
        const className = index === 'COLLSCAN' ? 'index-badge collscan' : 'index-badge';
        return `<span class="${className}">${index}</span>`;
    }).join(' ');
}

function truncateText(text, maxLength) {
    if (text.length <= maxLength) return text;
    return text.substring(0, maxLength) + '...';
}

async function analyzeReplicaSet() {
    if (!currentFileId) return;
    
    showLoading('Analyzing replica set...');
    
    try {
        const response = await fetch(`/api/analyze/${currentFileId}/replica-set`, { method: 'POST' });
        const result = await response.json();
        
        if (result.status === 'success') {
            displayReplicaSetData(result.data);
        } else {
            showToast('error', 'Replica set analysis failed');
        }
    } catch (error) {
        showToast('error', `Analysis failed: ${error.message}`);
    } finally {
        hideLoading();
    }
}

function displayReplicaSetData(data) {
    const container = document.getElementById('replicaSetContent');
    
    let html = '';
    
    // Node status
    if (Object.keys(data.node_status).length > 0) {
        html += `
            <div class="info-card">
                <h3><i class="fas fa-server"></i> Current Node Status</h3>
                <div class="info-details">
                    ${Object.entries(data.node_status).map(([host, status]) => `
                        <p><strong>${host}:</strong> ${status.state} (${status.timestamp})</p>
                    `).join('')}
                </div>
            </div>
        `;
    }
    
    // Configuration
    if (data.configs.length > 0) {
        const latestConfig = data.configs[data.configs.length - 1];
        const configId = 'config-' + Date.now();
        html += `
            <div class="info-card">
                <h3><i class="fas fa-cog"></i> Replica Set Configuration</h3>
                <p><strong>Timestamp:</strong> ${latestConfig.timestamp}</p>
                <div class="json-viewer" id="${configId}">
                    <button class="json-copy-btn" onclick="copyJsonToClipboard('${configId}')">
                        <i class="fas fa-copy"></i> Copy
                    </button>
                    <pre>${syntaxHighlightJson(latestConfig.config)}</pre>
                </div>
            </div>
        `;
    }
    
    // State transitions
    if (data.states.length > 0) {
        html += `
            <div class="info-card">
                <h3><i class="fas fa-history"></i> Recent State Transitions</h3>
                <div class="info-details">
                    ${data.states.slice(-10).map(state => `
                        <p><strong>${state.host}:</strong> ${state.old_state} → ${state.new_state} (${state.timestamp})</p>
                    `).join('')}
                </div>
            </div>
        `;
    }
    
    if (!html) {
        html = '<p>No replica set information found in the log.</p>';
    }
    
    container.innerHTML = html;
}

async function analyzeClients() {
    if (!currentFileId) return;
    
    showLoading('Analyzing clients...');
    
    try {
        const response = await fetch(`/api/analyze/${currentFileId}/clients`, { method: 'POST' });
        const result = await response.json();
        
        if (result.status === 'success') {
            displayClientsData(result.data);
        } else {
            showToast('error', 'Client analysis failed');
        }
    } catch (error) {
        showToast('error', `Analysis failed: ${error.message}`);
    } finally {
        hideLoading();
    }
}

function displayClientsData(data) {
    const container = document.getElementById('clientsContent');
    
    if (Object.keys(data.clients).length === 0) {
        container.innerHTML = '<p>No client information found in the log.</p>';
        return;
    }
    
    const html = Object.entries(data.clients).map(([driverKey, clientInfo]) => `
        <div class="info-card">
            <h3><i class="fas fa-desktop"></i> ${driverKey}</h3>
            <div class="info-details">
                <p><strong>Driver:</strong> ${clientInfo.driver_name} v${clientInfo.driver_version}</p>
                ${clientInfo.app_name ? `<p><strong>Application:</strong> ${clientInfo.app_name}</p>` : ''}
                <p><strong>Connections:</strong> ${clientInfo.connections.length}</p>
                <p><strong>IP Addresses:</strong> ${clientInfo.ips.join(', ')}</p>
                ${clientInfo.os_name ? `<p><strong>OS:</strong> ${clientInfo.os_name} ${clientInfo.os_version}</p>` : ''}
                ${clientInfo.users.length > 0 ? `<p><strong>Users:</strong> ${clientInfo.users.join(', ')}</p>` : ''}
            </div>
        </div>
    `).join('');
    
    container.innerHTML = html;
}

// Trim functionality
async function trimLogFile() {
    if (!currentFileId) return;
    
    const fromDate = document.getElementById('fromDate').value;
    const untilDate = document.getElementById('untilDate').value;
    
    if (!fromDate && !untilDate) {
        showToast('error', 'Please specify at least a from or until date');
        return;
    }
    
    showLoading('Trimming log file...');
    
    try {
        const response = await fetch(`/api/trim/${currentFileId}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                from_date: fromDate || null,
                until_date: untilDate || null
            })
        });
        
        const result = await response.json();
        
        if (result.status === 'success') {
            displayTrimResult(result.data);
            showToast('success', 'Log file trimmed successfully');
            loadUploadedFiles(); // Refresh file list
        } else {
            showToast('error', result.message || 'Trimming failed');
        }
    } catch (error) {
        showToast('error', `Trimming failed: ${error.message}`);
    } finally {
        hideLoading();
    }
}

function displayTrimResult(data) {
    const container = document.getElementById('trimResult');
    
    container.innerHTML = `
        <h4><i class="fas fa-check-circle"></i> Trimming Complete</h4>
        <p><strong>New File:</strong> ${data.filename}</p>
        <p><strong>Total Lines Processed:</strong> ${data.total_lines.toLocaleString()}</p>
        <p><strong>Lines Included:</strong> ${data.included_lines.toLocaleString()}</p>
        <p><strong>Lines Skipped:</strong> ${data.skipped_lines.toLocaleString()}</p>
        ${data.start_date ? `<p><strong>Start Date:</strong> ${data.start_date}</p>` : ''}
        ${data.end_date ? `<p><strong>End Date:</strong> ${data.end_date}</p>` : ''}
        <div class="mt-20">
            <button class="btn btn-primary" onclick="selectFile('${data.new_file_id}')">
                <i class="fas fa-eye"></i> Analyze Trimmed File
            </button>
            <button class="btn btn-secondary" onclick="downloadFile('${data.new_file_id}')">
                <i class="fas fa-download"></i> Download Trimmed File
            </button>
        </div>
    `;
}

// Utility functions
function showLoading(text = 'Processing...') {
    const overlay = document.getElementById('loadingOverlay');
    const textEl = document.getElementById('loadingText');
    textEl.textContent = text;
    overlay.style.display = 'flex';
}

function hideLoading() {
    document.getElementById('loadingOverlay').style.display = 'none';
}

function showToast(type, message) {
    const container = document.getElementById('toastContainer');
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    
    const icon = type === 'success' ? 'fas fa-check-circle' : 
                 type === 'error' ? 'fas fa-exclamation-circle' : 
                 'fas fa-info-circle';
    
    toast.innerHTML = `
        <i class="${icon}"></i>
        <span>${message}</span>
    `;
    
    container.appendChild(toast);
    
    // Remove toast after 5 seconds
    setTimeout(() => {
        if (toast.parentNode) {
            toast.parentNode.removeChild(toast);
        }
    }, 5000);
}

// Table sorting functionality
function sortTable(table, columnIndex, tableName) {
    const tbody = table.querySelector('tbody');
    const rows = Array.from(tbody.querySelectorAll('tr'));
    const headers = table.querySelectorAll('th');
    
    // Determine sort direction
    let direction = 'asc';
    if (currentSort.table === tableName && currentSort.column === columnIndex) {
        direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
    }
    
    // Update sort state
    currentSort = { table: tableName, column: columnIndex, direction: direction };
    
    // Clear previous sort indicators
    headers.forEach(header => {
        header.classList.remove('sort-asc', 'sort-desc');
    });
    
    // Add sort indicator to current column
    const currentHeader = headers[columnIndex];
    currentHeader.classList.add(direction === 'asc' ? 'sort-asc' : 'sort-desc');
    
    // Sort rows
    rows.sort((a, b) => {
        const aValue = getCellValue(a, columnIndex);
        const bValue = getCellValue(b, columnIndex);
        
        // Handle numeric values
        const aNum = parseFloat(aValue.replace(/[^\d.-]/g, ''));
        const bNum = parseFloat(bValue.replace(/[^\d.-]/g, ''));
        
        if (!isNaN(aNum) && !isNaN(bNum)) {
            return direction === 'asc' ? aNum - bNum : bNum - aNum;
        }
        
        // Handle string values
        return direction === 'asc' 
            ? aValue.localeCompare(bValue)
            : bValue.localeCompare(aValue);
    });
    
    // Re-append sorted rows
    rows.forEach(row => tbody.appendChild(row));
}

function getCellValue(row, columnIndex) {
    const cell = row.cells[columnIndex];
    return cell ? cell.textContent.trim() : '';
}

function makeSortable(table, tableName) {
    const headers = table.querySelectorAll('th');
    headers.forEach((header, index) => {
        header.classList.add('sortable');
        header.addEventListener('click', () => {
            sortTable(table, index, tableName);
        });
    });
}

// JSON syntax highlighting and utilities
function syntaxHighlightJson(obj) {
    const jsonString = JSON.stringify(obj, null, 2);
    
    return jsonString
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
            let cls = 'json-number';
            if (/^"/.test(match)) {
                if (/:$/.test(match)) {
                    cls = 'json-key';
                } else {
                    cls = 'json-string';
                }
            } else if (/true|false/.test(match)) {
                cls = 'json-boolean';
            } else if (/null/.test(match)) {
                cls = 'json-null';
            }
            return '<span class="' + cls + '">' + match + '</span>';
        });
}

function copyJsonToClipboard(elementId) {
    const element = document.getElementById(elementId);
    const pre = element.querySelector('pre');
    
    if (pre) {
        // Get the text content without HTML tags
        const text = pre.textContent || pre.innerText;
        
        // Use modern clipboard API if available
        if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(text).then(() => {
                showToast('success', 'Configuration copied to clipboard!');
            }).catch(() => {
                fallbackCopyToClipboard(text);
            });
        } else {
            fallbackCopyToClipboard(text);
        }
    }
}

function fallbackCopyToClipboard(text) {
    // Fallback method for older browsers
    const textArea = document.createElement('textarea');
    textArea.value = text;
    textArea.style.position = 'fixed';
    textArea.style.left = '-9999px';
    textArea.style.top = '-9999px';
    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();
    
    try {
        document.execCommand('copy');
        showToast('success', 'Configuration copied to clipboard!');
    } catch (err) {
        showToast('error', 'Failed to copy to clipboard');
    }
    
    document.body.removeChild(textArea);
}

// Query examples functionality
function showQueryExamplesFromElement(element) {
    const namespace = element.dataset.namespace;
    const operation = element.dataset.operation;
    const encodedPattern = element.dataset.pattern;
    const rowIndex = parseInt(element.dataset.rowIndex);
    
    showQueryExamples(namespace, operation, encodedPattern, rowIndex);
}

async function showQueryExamples(namespace, operation, encodedPattern, rowIndex) {
    if (!currentFileId) return;
    
    const pattern = decodeURIComponent(encodedPattern);
    
    console.log('🔍 Requesting query examples:', { namespace, operation, pattern });
    
    showLoading('Loading query examples...');
    
    try {
        const response = await fetch(`/api/analyze/${currentFileId}/query-examples`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                namespace: namespace,
                operation: operation,
                pattern: pattern
            })
        });
        
        const result = await response.json();
        
        console.log('📊 Query examples result:', result);
        
        if (result.status === 'success') {
            displayQueryExamples(result.data, rowIndex);
        } else {
            showToast('error', 'Failed to load query examples');
        }
    } catch (error) {
        showToast('error', `Failed to load query examples: ${error.message}`);
    } finally {
        hideLoading();
    }
}

// Store current query info for AI recommendations
let currentQueryForAI = null;
let currentQueryExamples = null;

function displayQueryExamples(data, rowIndex) {
    // Store query info and examples for AI button
    if (currentQueriesData && currentQueriesData.queries[rowIndex]) {
        currentQueryForAI = {
            ...currentQueriesData.queries[rowIndex],
            rowIndex: rowIndex
        };
    }
    
    // Store the actual query examples
    currentQueryExamples = data;
    
    // Remove any existing examples
    const existingExamples = document.querySelector('.query-examples');
    if (existingExamples) {
        existingExamples.remove();
    }
    
    // Find the table and insert after the clicked row
    const table = document.querySelector('#queriesTable table');
    const rows = table.querySelectorAll('tbody tr');
    const targetRow = rows[rowIndex];
    
    if (!targetRow) return;
    
    // Create examples container
    const examplesContainer = document.createElement('div');
    examplesContainer.className = 'query-examples';
    examplesContainer.style.position = 'relative';
    
    let examplesHtml = `
        <div class="examples-header">
            <h4>
                <i class="fas fa-code"></i> 
                Query Examples for ${data.namespace}.${data.operation}
            </h4>
            <div class="examples-actions">
                <button class="btn btn-ai" onclick="getIndexRecommendationForQuery(${rowIndex})">
                    <i class="fas fa-magic"></i> Get AI Index Recommendation
                </button>
                <button class="close-examples" onclick="closeQueryExamples()">
                    <i class="fas fa-times"></i> Close
                </button>
            </div>
        </div>
    `;
    
    if (data.examples.length === 0) {
        examplesHtml += '<p>No query examples found for this pattern.</p>';
    } else {
        data.examples.forEach((example, index) => {
            const timestamp = new Date(example.timestamp).toLocaleString();
            // Parse the full log entry from raw log line
            let fullLogEntry = {};
            try {
                fullLogEntry = JSON.parse(example.raw_log_line);
            } catch (e) {
                fullLogEntry = { error: "Could not parse log entry", raw: example.raw_log_line };
            }
            
            examplesHtml += `
                <div class="query-example">
                    <div class="query-example-header">
                        <strong>Example ${index + 1}</strong>
                        <div class="query-example-meta">
                            <span><i class="fas fa-clock"></i> ${timestamp}</span>
                            <span><i class="fas fa-stopwatch"></i> ${example.duration_ms}ms</span>
                            <span><i class="fas fa-search"></i> ${example.plan_summary}</span>
                        </div>
                    </div>
                    <div class="query-example-command">
                        <div class="json-viewer json-viewer-compact">
                            <button class="json-copy-btn" onclick="copyQueryExample(${index})">
                                <i class="fas fa-copy"></i> Copy
                            </button>
                            <pre id="example-${index}">${syntaxHighlightJson(fullLogEntry)}</pre>
                        </div>
                    </div>
                </div>
            `;
        });
    }
    
    examplesContainer.innerHTML = examplesHtml;
    
    // Insert after the table
    const tableContainer = document.getElementById('queriesTable');
    tableContainer.appendChild(examplesContainer);
    
    // Scroll to examples
    examplesContainer.scrollIntoView({ behavior: 'smooth' });
}

function closeQueryExamples() {
    const examples = document.querySelector('.query-examples');
    if (examples) {
        examples.remove();
    }
}

function copyQueryExample(exampleIndex) {
    const element = document.getElementById(`example-${exampleIndex}`);
    if (element) {
        const text = element.textContent || element.innerText;
        
        if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(text).then(() => {
                showToast('success', 'Query copied to clipboard!');
            }).catch(() => {
                fallbackCopyToClipboard(text);
            });
        } else {
            fallbackCopyToClipboard(text);
        }
    }
}

// Time Series Analysis Functions
async function analyzeTimeSeries() {
    if (!currentFileId) return;
    
    const namespace = document.getElementById('timeseriesNamespaceFilter').value;
    
    showLoading('Analyzing time-series data...');
    
    try {
        let url = `/api/analyze/${currentFileId}/timeseries`;
        if (namespace) {
            url += `?namespace=${encodeURIComponent(namespace)}`;
        }
        
        const response = await fetch(url, { method: 'POST' });
        const result = await response.json();
        
        if (result.status === 'success') {
            displayTimeSeriesData(result.data);
        } else {
            showToast('error', 'Time-series analysis failed');
        }
    } catch (error) {
        showToast('error', `Analysis failed: ${error.message}`);
    } finally {
        hideLoading();
    }
}

function displayTimeSeriesData(data) {
    // Update namespace filter
    const namespaceFilter = document.getElementById('timeseriesNamespaceFilter');
    namespaceFilter.innerHTML = '<option value="">All namespaces</option>';
    data.unique_namespaces.forEach(ns => {
        const option = document.createElement('option');
        option.value = ns;
        option.textContent = ns;
        namespaceFilter.appendChild(option);
    });
    
    // Create slow queries plot
    createSlowQueriesPlot(data.slow_queries);
    
    // Create connections plot
    createConnectionsPlot(data.connections);
    
    // Create errors plot
    createErrorsPlot(data.errors);
    
    // Display aggregated tables
    displayAggregatedQueriesTable(data.aggregated_queries);
    displayAggregatedErrorsTable(data.aggregated_errors);
    
    // Show info if data was sampled
    if (data.sampled) {
        showToast('info', `Displaying 10,000 sampled queries out of ${data.total_slow_queries.toLocaleString()} total`);
    }
}

function createSlowQueriesPlot(slowQueries) {
    if (!slowQueries || slowQueries.length === 0) {
        document.getElementById('slowQueriesPlot').innerHTML = '<p style="text-align: center; padding: 20px;">No slow queries found in the log file.</p>';
        return;
    }
    
    // Group by namespace for different traces
    const tracesByNamespace = {};
    slowQueries.forEach(q => {
        if (!tracesByNamespace[q.namespace]) {
            tracesByNamespace[q.namespace] = {
                x: [],
                y: [],
                customdata: [],
                mode: 'markers',
                type: 'scatter',
                name: q.namespace,
                marker: { size: 8 }
            };
        }
        tracesByNamespace[q.namespace].x.push(q.timestamp);
        tracesByNamespace[q.namespace].y.push(q.duration_ms);
        tracesByNamespace[q.namespace].customdata.push({
            command: q.command,
            plan_summary: q.plan_summary,
            namespace: q.namespace
        });
    });
    
    const traces = Object.values(tracesByNamespace);
    
    const layout = {
        title: '',
        xaxis: {
            title: 'Timestamp',
            type: 'date',
            rangeslider: { visible: false }
        },
        yaxis: {
            title: 'Duration (ms)',
            type: 'linear'
        },
        hovermode: 'closest',
        showlegend: true,
        legend: {
            orientation: 'h',
            y: -0.2
        },
        margin: { t: 20, r: 20, b: 80, l: 60 }
    };
    
    const config = {
        responsive: true,
        displayModeBar: true,
        displaylogo: false
    };
    
    Plotly.newPlot('slowQueriesPlot', traces, layout, config);
    
    // Add click event to show query details
    document.getElementById('slowQueriesPlot').on('plotly_click', function(eventData) {
        const point = eventData.points[0];
        const customdata = point.customdata;
        displayQueryDetails(
            point.x,
            point.y,
            customdata.namespace,
            customdata.command,
            customdata.plan_summary
        );
    });
    
    // Add zoom sync event
    document.getElementById('slowQueriesPlot').on('plotly_relayout', function(eventData) {
        syncTimeSeriesZoom('slowQueriesPlot', eventData);
    });
}

function createConnectionsPlot(connections) {
    if (!connections || connections.length === 0) {
        document.getElementById('connectionsPlot').innerHTML = '<p style="text-align: center; padding: 20px;">No connection data found in the log file.</p>';
        return;
    }
    
    const trace = {
        x: connections.map(c => c.timestamp),
        y: connections.map(c => c.connection_count),
        mode: 'lines',
        type: 'scatter',
        name: 'Connection Count',
        line: {
            color: '#667eea',
            width: 2
        },
        fill: 'tozeroy',
        fillcolor: 'rgba(102, 126, 234, 0.2)'
    };
    
    const layout = {
        title: '',
        xaxis: {
            title: 'Timestamp',
            type: 'date',
            rangeslider: { visible: false }
        },
        yaxis: {
            title: 'Connection Count',
            type: 'linear'
        },
        hovermode: 'x unified',
        showlegend: false,
        margin: { t: 20, r: 20, b: 80, l: 60 }
    };
    
    const config = {
        responsive: true,
        displayModeBar: true,
        displaylogo: false
    };
    
    Plotly.newPlot('connectionsPlot', [trace], layout, config);
    
    // Add zoom sync event
    document.getElementById('connectionsPlot').on('plotly_relayout', function(eventData) {
        syncTimeSeriesZoom('connectionsPlot', eventData);
    });
}

function createErrorsPlot(errors) {
    if (!errors || errors.length === 0) {
        document.getElementById('errorsPlot').innerHTML = '<p style="text-align: center; padding: 20px;">No errors or warnings found in the log file.</p>';
        return;
    }
    
    // Group by message
    const tracesByMessage = {};
    errors.forEach(e => {
        const msgKey = e.message;
        if (!tracesByMessage[msgKey]) {
            tracesByMessage[msgKey] = {
                x: [],
                y: [],
                mode: 'markers',
                type: 'scatter',
                name: msgKey.substring(0, 50) + (msgKey.length > 50 ? '...' : ''),
                marker: { 
                    size: 10,
                    symbol: 'diamond'
                }
            };
        }
        tracesByMessage[msgKey].x.push(e.timestamp);
        tracesByMessage[msgKey].y.push(msgKey);
    });
    
    const traces = Object.values(tracesByMessage);
    
    const layout = {
        title: '',
        xaxis: {
            title: 'Timestamp',
            type: 'date',
            rangeslider: { visible: false }
        },
        yaxis: {
            title: 'Error/Warning Message',
            type: 'category'
        },
        hovermode: 'closest',
        showlegend: true,
        legend: {
            orientation: 'h',
            y: -0.2
        },
        margin: { t: 20, r: 20, b: 80, l: 60 }
    };
    
    const config = {
        responsive: true,
        displayModeBar: true,
        displaylogo: false
    };
    
    Plotly.newPlot('errorsPlot', traces, layout, config);
    
    // Add zoom sync event
    document.getElementById('errorsPlot').on('plotly_relayout', function(eventData) {
        syncTimeSeriesZoom('errorsPlot', eventData);
    });
}

// Global flag to prevent infinite zoom sync loops
let isTimeSeriesSyncing = false;

function syncTimeSeriesZoom(sourceId, eventData) {
    // Prevent infinite loop when syncing
    if (isTimeSeriesSyncing) return;
    
    // Check if this is a relevant event (zoom, pan, autoscale, reset)
    const hasXRange = eventData['xaxis.range[0]'] || eventData['xaxis.range'] || 
                      (eventData.xaxis && eventData.xaxis.range);
    const hasAutoScale = eventData['xaxis.autorange'];
    
    if (!hasXRange && !hasAutoScale) return;
    
    isTimeSeriesSyncing = true;
    
    // Build the relayout object based on what changed
    let relayoutData = {};
    
    // Handle zoom/pan (explicit range)
    if (eventData['xaxis.range[0]'] && eventData['xaxis.range[1]']) {
        relayoutData['xaxis.range'] = [eventData['xaxis.range[0]'], eventData['xaxis.range[1]']];
    } else if (eventData['xaxis.range']) {
        relayoutData['xaxis.range'] = eventData['xaxis.range'];
    } else if (eventData.xaxis && eventData.xaxis.range) {
        relayoutData['xaxis.range'] = eventData.xaxis.range;
    }
    
    // Handle autorange (reset/double-click)
    if (eventData['xaxis.autorange'] !== undefined) {
        relayoutData['xaxis.autorange'] = eventData['xaxis.autorange'];
    }
    
    // If no relevant changes, exit
    if (Object.keys(relayoutData).length === 0) {
        isTimeSeriesSyncing = false;
        return;
    }
    
    // Update all plots except the source
    const plotIds = ['slowQueriesPlot', 'connectionsPlot', 'errorsPlot'];
    let syncPromises = [];
    
    plotIds.forEach(plotId => {
        if (plotId !== sourceId) {
            const plotElement = document.getElementById(plotId);
            if (plotElement && plotElement.data) {
                syncPromises.push(
                    Plotly.relayout(plotId, relayoutData).catch(err => {
                        console.warn(`Failed to sync ${plotId}:`, err);
                    })
                );
            }
        }
    });
    
    // Wait for all sync operations to complete
    Promise.all(syncPromises).finally(() => {
        setTimeout(() => {
            isTimeSeriesSyncing = false;
        }, 100);
    });
}

function displayQueryDetails(timestamp, duration, namespace, command, planSummary) {
    const detailsContainer = document.getElementById('queryDetails');
    const detailsContent = document.getElementById('queryDetailsContent');
    
    detailsContent.innerHTML = `
        <div class="info-details">
            <p><strong>Timestamp:</strong> ${new Date(timestamp).toLocaleString()}</p>
            <p><strong>Duration:</strong> ${duration} ms</p>
            <p><strong>Namespace:</strong> ${namespace}</p>
            <p><strong>Plan Summary:</strong> ${planSummary}</p>
        </div>
        <div class="json-viewer">
            <button class="json-copy-btn" onclick="copyCommandToClipboard()">
                <i class="fas fa-copy"></i> Copy Command
            </button>
            <pre id="commandJson">${syntaxHighlightJson(command)}</pre>
        </div>
    `;
    
    detailsContainer.style.display = 'block';
    detailsContainer.scrollIntoView({ behavior: 'smooth' });
}

function closeQueryDetails() {
    document.getElementById('queryDetails').style.display = 'none';
}

function copyCommandToClipboard() {
    const element = document.getElementById('commandJson');
    if (element) {
        const text = element.textContent || element.innerText;
        
        if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(text).then(() => {
                showToast('success', 'Command copied to clipboard!');
            }).catch(() => {
                fallbackCopyToClipboard(text);
            });
        } else {
            fallbackCopyToClipboard(text);
        }
    }
}

function displayAggregatedQueriesTable(aggregatedQueries) {
    const container = document.getElementById('aggregatedQueriesTable');
    
    if (!aggregatedQueries || aggregatedQueries.length === 0) {
        container.innerHTML = '<p>No slow queries to aggregate.</p>';
        return;
    }
    
    const table = document.createElement('table');
    table.className = 'timeseries-table';
    
    table.innerHTML = `
        <thead>
            <tr>
                <th>Namespace</th>
                <th>Count</th>
                <th>Mean Duration (ms)</th>
            </tr>
        </thead>
        <tbody>
            ${aggregatedQueries.map(q => `
                <tr>
                    <td>${q.namespace}</td>
                    <td>${q.count}</td>
                    <td>${q.mean_duration_ms}</td>
                </tr>
            `).join('')}
        </tbody>
    `;
    
    container.innerHTML = '';
    container.appendChild(table);
}

function displayAggregatedErrorsTable(aggregatedErrors) {
    const container = document.getElementById('aggregatedErrorsTable');
    
    if (!aggregatedErrors || aggregatedErrors.length === 0) {
        container.innerHTML = '<p>No errors or warnings to display.</p>';
        return;
    }
    
    const table = document.createElement('table');
    table.className = 'timeseries-table';
    
    table.innerHTML = `
        <thead>
            <tr>
                <th>Message</th>
                <th>Count</th>
            </tr>
        </thead>
        <tbody>
            ${aggregatedErrors.map(e => `
                <tr>
                    <td>${e.message}</td>
                    <td>${e.count}</td>
                </tr>
            `).join('')}
        </tbody>
    `;
    
    container.innerHTML = '';
    container.appendChild(table);
} 

function showLLMInstallationModal(statusData) {
    const modal = document.createElement('div');
    modal.className = 'recommendations-modal';
    modal.style.display = 'flex';
    
    let instructionsHTML = '';
    if (statusData.instructions && statusData.instructions.length > 0) {
        instructionsHTML = statusData.instructions.map(instruction => `
            <div class="installation-step">
                <h4>Step ${instruction.step}: ${instruction.action}</h4>
                <p>${instruction.description}</p>
                <div class="command-box">
                    <code>${instruction.command}</code>
                    <button class="copy-btn" onclick="copyToClipboard('${instruction.command}')">
                        <i class="fas fa-copy"></i> Copy
                    </button>
                </div>
            </div>
        `).join('');
    }
    
    modal.innerHTML = `
        <div class="recommendations-content" style="max-width: 700px;">
            <div class="recommendations-header">
                <h3><i class="fas fa-robot"></i> AI Index Advisor Setup Required</h3>
                <button class="recommendations-close" onclick="this.closest('.recommendations-modal').remove()">
                    <i class="fas fa-times"></i>
                </button>
            </div>
            
            <div class="installation-info">
                <p><strong>The AI-powered index advisor is not yet configured.</strong></p>
                <p>To enable intelligent index recommendations with LLM-enhanced analysis, please complete the following steps:</p>
                
                ${instructionsHTML}
                
                <div class="installation-note">
                    <p><i class="fas fa-info-circle"></i> <strong>Note:</strong></p>
                    <ul>
                        <li>The LLM runs <strong>locally</strong> on your machine (no data sent externally)</li>
                        <li>Model size: ~300MB (one-time download)</li>
                        <li>Rule-based recommendations work without LLM, but AI provides better insights</li>
                    </ul>
                </div>
                
                <div class="installation-actions">
                    <button class="btn" onclick="this.closest('.recommendations-modal').remove()">
                        <i class="fas fa-times"></i> Cancel
                    </button>
                    <button class="btn btn-primary" onclick="window.open('https://github.com/abetlen/llama-cpp-python', '_blank')">
                        <i class="fas fa-external-link-alt"></i> View Installation Guide
                    </button>
                </div>
            </div>
        </div>
    `;
    
    document.body.appendChild(modal);
}

function copyToClipboard(text) {
    navigator.clipboard.writeText(text).then(() => {
        showToast('success', 'Command copied to clipboard!');
    }).catch(err => {
        console.error('Failed to copy:', err);
        showToast('error', 'Failed to copy command');
    });
}

// AI Index Recommendations for Individual Queries
async function getIndexRecommendationForQuery(queryIndex) {
    if (!currentQueriesData || queryIndex === null || queryIndex === undefined) {
        alert('No query selected');
        return;
    }
    
    const query = currentQueriesData.queries[queryIndex];
    
    // Check LLM status first
    showLoading('Checking AI system...');
    try {
        const statusResponse = await fetch('/api/llm-status');
        const statusResult = await statusResponse.json();
        
        if (statusResult.status === 'success' && statusResult.data.installation_required) {
            hideLoading();
            showLLMInstallationModal(statusResult.data);
            return;
        }
    } catch (error) {
        console.error('Failed to check LLM status:', error);
        // Continue anyway - will use rule-based recommendations
    }
    
    // Use actual query example if available, otherwise use simplified pattern
    let rawLogLine = null;
    if (currentQueryExamples && currentQueryExamples.examples && currentQueryExamples.examples.length > 0) {
        rawLogLine = currentQueryExamples.examples[0].raw_log_line;
    }
    
    showLoading('Analyzing query pattern...');
    
    try {
        // Build single-query payload for the advisor
        const singleQueryData = {
            namespace: query.namespace,
            operation: query.operation,
            pattern: query.pattern,  // Send simplified pattern for fallback
            raw_log_line: rawLogLine,  // Send actual log line for detailed analysis
            stats: {
                count: query.count,
                mean: query.mean_ms,
                min: query.min_ms,
                max: query.max_ms,
                percentile_95: query.percentile_95_ms,
                indexes: query.indexes || []
            }
        };
        
        // Call the recommendation API with single_query=true for LLM enhancement
        const response = await fetch(`/api/analyze/${currentFileId}/index-recommendations?single_query=true`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(singleQueryData)
        });
        
        const result = await response.json();
        
        console.log('📊 API Response:', result);
        
        if (result.status === 'success') {
            // For single query analysis, we expect exactly one recommendation
            const recommendations = result.data.recommendations;
            
            if (recommendations && recommendations.length > 0) {
                console.log('✅ Displaying recommendation:', recommendations[0]);
                displaySingleRecommendation(recommendations[0], query);
            } else {
                console.log('⚠️ No recommendations returned');
                showNoRecommendationForQuery(query);
            }
        } else {
            throw new Error(result.message || 'Failed to get recommendation');
        }
    } catch (error) {
        console.error('Error getting recommendation:', error);
        alert('Failed to get index recommendation: ' + error.message);
    } finally {
        hideLoading();
    }
}

async function getDetailedAnalysis(buttonElement) {
    const namespace = buttonElement.dataset.namespace;
    const operation = buttonElement.dataset.operation;
    const pattern = JSON.parse(buttonElement.dataset.pattern);
    const stats = JSON.parse(buttonElement.dataset.stats);
    
    // Get raw log line if available (from current query examples)
    let rawLogLine = null;
    if (currentQueryExamples && currentQueryExamples.examples && currentQueryExamples.examples.length > 0) {
        rawLogLine = currentQueryExamples.examples[0].raw_log_line;
    }
    
    // Disable button and show loading
    const originalHTML = buttonElement.innerHTML;
    buttonElement.disabled = true;
    buttonElement.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Analyzing... (this may take 10-30 seconds)';
    
    // Expand the explanation section if collapsed
    const explanationToggle = buttonElement.closest('.explanation-header-with-button').querySelector('.explanation-toggle');
    const explanationContent = buttonElement.closest('.recommendation-explanation').querySelector('.explanation-content');
    if (explanationContent.style.display === 'none') {
        toggleExplanation(explanationToggle);
    }
    
    try {
        const response = await fetch(`/api/analyze/${currentFileId}/detailed-analysis`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                namespace,
                operation,
                pattern,
                raw_log_line: rawLogLine,
                stats
            })
        });
        
        const result = await response.json();
        
        if (result.status === 'success') {
            const placeholder = buttonElement.closest('.recommendation-explanation').querySelector('.detailed-analysis-placeholder');
            
            placeholder.innerHTML = `
                <div class="detailed-analysis-result">
                    <div class="analysis-header">
                        <i class="fas fa-brain"></i> <strong>AI-Powered Detailed Analysis</strong>
                    </div>
                    ${result.data.is_optimized ? `
                        <div class="optimization-approved">
                            <i class="fas fa-check-circle"></i> Query is already optimized!
                        </div>
                    ` : ''}
                    <div class="analysis-content">
                        ${formatExplanation(result.data.explanation)}
                    </div>
                </div>
            `;
            
            // Hide the button after successful analysis
            buttonElement.style.display = 'none';
            
            showToast('success', 'Detailed AI analysis complete!');
        } else {
            throw new Error(result.message || 'Analysis failed');
        }
    } catch (error) {
        console.error('Failed to get detailed analysis:', error);
        showToast('error', `Failed to get detailed analysis: ${error.message}`);
        buttonElement.disabled = false;
        buttonElement.innerHTML = originalHTML;
    }
}

function toggleExplanation(headerElement) {
    const content = headerElement.parentElement.parentElement.querySelector('.explanation-content') || 
                    headerElement.nextElementSibling;
    const icon = headerElement.querySelector('i');
    const hint = headerElement.querySelector('.toggle-hint');
    
    if (content && content.style.display === 'none') {
        content.style.display = 'block';
        icon.className = 'fas fa-chevron-down';
        hint.textContent = '(click to collapse)';
    } else if (content) {
        content.style.display = 'none';
        icon.className = 'fas fa-chevron-right';
        hint.textContent = '(click to expand)';
    }
}

function formatExplanation(text) {
    if (!text) return '<p>No explanation provided.</p>';
    
    // Convert **Bold Text:** headers to styled divs
    text = text.replace(/\*\*([^*]+):\*\*/g, '<div class="explanation-section"><strong>$1:</strong></div>');
    
    // Convert remaining **bold** text
    text = text.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
    
    // Convert line breaks to paragraphs
    const paragraphs = text.split('\n\n').filter(p => p.trim());
    
    return paragraphs.map(para => {
        para = para.trim();
        // If it starts with a section header div, don't wrap in <p>
        if (para.startsWith('<div class="explanation-section">')) {
            return para;
        }
        return `<p>${para.replace(/\n/g, '<br>')}</p>`;
    }).join('');
}

function displayOptimizedQuery(rec, query) {
    const stats = rec.stats || {};
    
    const modal = document.createElement('div');
    modal.className = 'recommendations-modal';
    modal.onclick = (e) => {
        if (e.target === modal) {
            modal.remove();
        }
    };
    
    const content = document.createElement('div');
    content.className = 'recommendations-content';
    content.onclick = (e) => e.stopPropagation();
    
    content.innerHTML = `
        <div class="recommendations-header">
            <h2><i class="fas fa-check-circle" style="color: #27ae60;"></i> Query Already Optimized</h2>
            <button class="recommendations-close" onclick="this.closest('.recommendations-modal').remove()">
                <i class="fas fa-times"></i>
            </button>
        </div>
        
        <div class="optimized-query-info">
            <div class="query-info-header">
                <h3>${rec.namespace} - ${rec.operation}</h3>
                <div class="query-stats-inline">
                    <span><i class="fas fa-repeat"></i> ${stats.count}× executions</span>
                    <span><i class="fas fa-clock"></i> ${stats.mean_ms?.toFixed(1)}ms avg</span>
                    <span><i class="fas fa-chart-line"></i> ${stats.p95_ms?.toFixed(1)}ms p95</span>
                </div>
            </div>
            
            <div class="current-index-display">
                <strong>Current Index:</strong> <code>${rec.current_index}</code>
            </div>
            
            <div class="optimization-status">
                <div class="status-badge status-good">
                    <i class="fas fa-thumbs-up"></i> No Action Needed
                </div>
                <p>This query is already well-optimized and follows MongoDB best practices.</p>
            </div>
            
            <div class="recommendation-explanation">
                <h4 class="explanation-toggle" onclick="toggleExplanation(this)">
                    <i class="fas fa-chevron-right"></i> Analysis Details
                    <span class="toggle-hint">(click to expand)</span>
                </h4>
                <div class="explanation-content" style="display: none;">
                    ${formatExplanation(rec.explanation || 'Query is already well-optimized.')}
                </div>
            </div>
        </div>
    `;
    
    modal.appendChild(content);
    document.body.appendChild(modal);
}

function displaySingleRecommendation(rec, query) {
    // Check if query is already optimized
    if (rec.is_optimized) {
        displayOptimizedQuery(rec, query);
        return;
    }
    
    const priority = rec.priority_level || 'MEDIUM';
    const stats = rec.stats || {};
    const recommendation = rec.recommendation || {};
    
    // Create modal
    const modal = document.createElement('div');
    modal.className = 'recommendations-modal';
    modal.onclick = (e) => {
        if (e.target === modal) closeRecommendations();
    };
    
    const content = document.createElement('div');
    content.className = 'recommendations-content';
    content.onclick = (e) => e.stopPropagation();
    
    const cmdId = 'cmd-single-' + Math.random().toString(36).substr(2, 9);
    
    content.innerHTML = `
        <div class="recommendations-header">
            <h2>
                <i class="fas fa-magic"></i> 
                Index Recommendation
            </h2>
            <button class="recommendations-close" onclick="closeRecommendations()">
                <i class="fas fa-times"></i>
            </button>
        </div>
        
        <div class="recommendation-card priority-${priority}">
            <span class="recommendation-priority ${priority}">${priority} Priority</span>
            
            <div class="recommendation-query-info">
                <h4>${rec.namespace} - ${rec.operation}</h4>
                <div class="recommendation-stats">
                    <span><i class="fas fa-redo"></i> ${stats.count || 0}× executed</span>
                    <span><i class="fas fa-clock"></i> ${stats.mean_ms || 0}ms avg</span>
                    <span><i class="fas fa-chart-line"></i> ${stats.p95_ms || 0}ms p95</span>
                </div>
                <div class="recommendation-pattern">${rec.pattern}</div>
            </div>
            
            <div class="recommendation-index">
                <h5><i class="fas fa-database"></i> Recommended Index</h5>
                <div class="recommendation-command">
                    <code id="${cmdId}">${recommendation.command || ''}</code>
                    <button class="copy-btn" onclick="copyIndexCommand('${cmdId}')">
                        <i class="fas fa-copy"></i> Copy
                    </button>
                </div>
            </div>
            
            <div class="recommendation-explanation">
                <div class="explanation-header-with-button">
                    <h4 class="explanation-toggle" onclick="toggleExplanation(this)">
                        <i class="fas fa-chevron-right"></i> Why This Index Is Recommended
                        <span class="toggle-hint">(click to expand)</span>
                    </h4>
                    <button class="btn btn-detailed-analysis" onclick="getDetailedAnalysis(this)" 
                            data-namespace="${rec.namespace}" 
                            data-operation="${rec.operation}"
                            data-pattern='${JSON.stringify(rec.pattern).replace(/'/g, "&apos;")}'
                            data-stats='${JSON.stringify(rec.stats).replace(/'/g, "&apos;")}'
                            data-current-index="${rec.current_index}">
                        <i class="fas fa-brain"></i> Get Detailed AI Explanation
                    </button>
                </div>
                <div class="explanation-content" style="display: none;">
                    ${formatExplanation(recommendation.reason || 'No reason provided')}
                    <div class="detailed-analysis-placeholder"></div>
                </div>
            </div>
            
            ${recommendation.additional_tip ? `
                <div class="llm-tip">
                    ${recommendation.additional_tip}
                </div>
            ` : ''}
        </div>
    `;
    
    modal.appendChild(content);
    document.body.appendChild(modal);
}

function showNoRecommendationForQuery(query) {
    const modal = document.createElement('div');
    modal.className = 'recommendations-modal';
    modal.onclick = (e) => {
        if (e.target === modal) closeRecommendations();
    };
    
    const content = document.createElement('div');
    content.className = 'recommendations-content';
    content.onclick = (e) => e.stopPropagation();
    
    content.innerHTML = `
        <div class="recommendations-header">
            <h2><i class="fas fa-magic"></i> Index Recommendation</h2>
            <button class="recommendations-close" onclick="closeRecommendations()">
                <i class="fas fa-times"></i>
            </button>
        </div>
        
        <div class="no-recommendations">
            <i class="fas fa-check-circle"></i>
            <h3>This Query Looks Good!</h3>
            <p><strong>${query.namespace} - ${query.operation}</strong></p>
            <p style="margin-top: 15px; color: #27ae60;">
                ✓ No critical optimization needed for this query pattern
            </p>
            <p style="margin-top: 10px; font-size: 0.9rem; color: #666;">
                Current index: <strong>${formatIndexes(query.indexes)}</strong>
            </p>
        </div>
    `;
    
    modal.appendChild(content);
    document.body.appendChild(modal);
}

function closeRecommendations() {
    const modal = document.querySelector('.recommendations-modal');
    if (modal) {
        modal.remove();
    }
}

function copyIndexCommand(elementId) {
    const element = document.getElementById(elementId);
    if (!element) return;
    
    const text = element.textContent || element.innerText;
    
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(() => {
            showToast('success', 'Index command copied to clipboard!');
        }).catch(() => {
            fallbackCopyToClipboard(text);
        });
    } else {
        fallbackCopyToClipboard(text);
    }
}
