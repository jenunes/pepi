// Global state
let currentFileId = null;
let charts = {};

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
    
    mongoInfo.innerHTML = `
        <p><strong>MongoDB Version:</strong> ${data.db_version || 'N/A'}</p>
        <p><strong>OS Version:</strong> ${data.os_version || 'N/A'}</p>
        <p><strong>Kernel Version:</strong> ${data.kernel_version || 'N/A'}</p>
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

function displayQueriesData(data) {
    const statsGrid = document.getElementById('queryStats');
    const tableContainer = document.getElementById('queriesTable');
    
    // Calculate stats
    const totalQueries = data.queries.reduce((sum, q) => sum + q.count, 0);
    const avgExecutionTime = data.queries.reduce((sum, q) => sum + q.mean_ms, 0) / data.queries.length;
    const slowestQuery = Math.max(...data.queries.map(q => q.max_ms));
    const collscans = data.queries.filter(q => q.indexes.includes('COLLSCAN')).length;
    
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
            ${data.queries.map(query => `
                <tr>
                    <td>${query.namespace}</td>
                    <td>${query.operation}</td>
                    <td><span class="pattern-text">${truncateText(query.pattern, 50)}</span></td>
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
        html += `
            <div class="info-card">
                <h3><i class="fas fa-cog"></i> Replica Set Configuration</h3>
                <p><strong>Timestamp:</strong> ${latestConfig.timestamp}</p>
                <div class="code-block">${JSON.stringify(latestConfig.config, null, 2)}</div>
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