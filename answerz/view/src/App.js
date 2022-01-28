import './App.css';
import 'reactjs-popup/dist/index.css';
import spinner from './spinner.svg';
import React, {useEffect, useState, useReducer} from 'react';
import 'react-minimal-side-navigation/lib/ReactMinimalSideNavigation.css';
import {Container, Row, Col} from 'react-grid-system';
import FormControl from '@mui/material/FormControl';
import InputLabel from '@mui/material/InputLabel';
import Typography from '@mui/material/Typography';
import TextField from '@mui/material/TextField';
import ReactVoiceInput from 'react-voice-input';
import MenuItem from '@mui/material/MenuItem';
import Checkbox from '@mui/material/Checkbox';
import {Chart} from "react-google-charts";
import {DataGrid} from '@mui/x-data-grid';
import Button from '@mui/material/Button';
import Select from '@mui/material/Select';
import ReactCodeSnippet from 'react-code-snippet'
import {CSVReader} from 'react-papaparse'
import Modal from '@mui/material/Modal';
import Box from '@mui/material/Box';
import axios from 'axios'
import {
    createTheme,
    useTheme,
} from "@material-ui/core/styles";

const THEME = createTheme({
    typography: {
        // In Chinese and Japanese the characters are usually larger,
        // so a smaller fontsize may be appropriate.
        fontSize: 200,
    },

});

const formReducer = (state, event) => {
    return {
        ...state,
        [event.name]: event.value
    }
}


// Make sure to bind modal to your appElement (https://reactcommunity.org/react-modal/accessibility/)
console.log(document.getElementById('main'))

function App() {

    const buttonRef = React.createRef();
    // const theme = useTheme(THEME);
    const [getMessage, setGetMessage] = useState({})
    const [getTable, setGetTable] = useState({})
    const [getIsScript, setIsScript] = useState(false)
    const [getChartType, setChartType] = useState('ColumnChart')
    const [formData, setFormData] = useReducer(formReducer, {});
    const [submitting, setSubmitting] = useState(false);
    const [getError, setError] = useState(false);
    const [getListening, setListening] = useState(false);
    const [getColumns, setColumns] = useState([]);
    const [getDataTables, setDataTables] = useState([]);
    const [getScriptQueries, setScriptQueries] = useState([]);
    const [getScriptIndex, setScriptIndex] = useState(0);
    const [getFilters, setFilters] = useState([]);
    const [getState, setState] = useState({
        leftOpen: false,
        rightOpen: false,
    });
    const [childModalOpen, setChildModalOpen] = React.useState(false);
    const handleChildModalOpen = () => {
        setChildModalOpen(true);
    };
    const handleChildModalClose = () => {
        setChildModalOpen(false);
    };

    const [weight, setWeight] = React.useState('');

    const [getPrevQuery, setPrevQuery] = useState('')
    const versionNumber = '2.0.39'

    const [getInputState, setInputState] = useState('');

    useEffect(() => {
        axios.get('http://localhost:1234/columns').then(response => {
            setColumns(response)
        }).catch(error => {
            console.log(error)
        })
    }, [])


    useEffect(() => {
        axios.get('http://localhost:1234/tables').then(response => {
            setDataTables(response)
        }).catch(error => {
            console.log(error)
        })


    }, [])


    // let rows = []
    // let cols = []

    const handleWeightChange = event => {
        setWeight(event.target.value);
    }
    const handleSubmit = event => {
        event.preventDefault();
        submit(getInputState);
    }

    const handleChartSwitch = event => {
        if (getChartType === 'LineChart')
            setChartType('ColumnChart')
        else
            setChartType('LineChart')
    }

    const submit = (inputValue) => {
        if (inputValue.toLowerCase() == 'properties') {
            setOpen(true);
            return;
        }
        setSubmitting(true);
        axios.post('http://localhost:1234/answerz', {
            'text': inputValue.replace('  ', ' '),
            'prev_query': (inputValue.toLowerCase().includes('group') || inputValue.toLowerCase().includes('down by') || inputValue.toLowerCase().includes('out by')) ? getPrevQuery : ''
        })
            .then(response => {
                console.log("Responded", response)
                setGetTable(response)
                setSubmitting(false);
                setError(false);
                if (!response.data.follow_up) {
                    console.log('Updating prev query..')
                    setPrevQuery(inputValue);
                }
            }).catch((error) => {
            setSubmitting(false);
            setError(true);
        });
    }

    const handleChange = event => {
        setInputState(event.target.value.replace('  ', ' '));
    }

    const style = {
        position: 'absolute',
        top: '50%',
        left: '50%',
        transform: 'translate(-50%, -50%)',
        width: '20%',
        bgcolor: 'background.paper',
        border: '2px solid #000',
        boxShadow: 24,
        p: 4,
    };

    function update_table() {
        let table = getTable;
        table.message.append("")
    }

    function onResult(result) {
        setInputState(result.replace('  ', ' '));
    }

    function startListening() {
        setListening(true);
    }

    const handleSubmit2 = () => {
        setListening(false);
        setSubmitting(true);
        axios.post('http://localhost:1234/answerz', {
            'text': getInputState,
            'prev_query': (getInputState.toLowerCase().includes('group') || getInputState.toLowerCase().includes('down by') || getInputState.toLowerCase().includes('out by')) ? getPrevQuery : ''
        })
            .then(response => {
                console.log("Responded", response)
                setGetTable(response)
                setSubmitting(false);
                setError(false);
                if (!response.data.follow_up) {
                    console.log('Updating prev query..')
                    setPrevQuery(getInputState);
                }
            }).catch((error) => {
            setSubmitting(false);
            setError(true);
        });
    }

    const [open, setOpen] = React.useState(false);
    const handleOpen = () => setOpen(true);
    const handleClose = () => setOpen(false);
    const handleBoxChange = () => setGetTable(getTable);
    const handleScriptBoxChange = () => setIsScript(!getIsScript);

    function handleFilter() {
        let rows = getTable.data.distinct_values_table.rows
        let cols = getTable.data.distinct_values_table.cols
        console.log(cols)
        let selected = getFilters.map(i => rows[i - 1])
        console.log(selected)
        let input = selected[0].value;
        setInputState(selected[0].value)
        submit(input)
    }

    const toggleSidebar = (event) => {
        let key = `${event.currentTarget.parentNode.id}Open`;
        let otherKey = (key === 'leftOpen') ? 'rightOpen' : 'leftOpen';
        setState({[key]: !getState[key], [otherKey]: getState[otherKey]});
        console.log(getState);
    }

    const handleOnDrop = (data) => {
        let column_headers = [];
        let script_queries = [];
        data.forEach((value, i) => {
            if (i === 0) {
                column_headers = value.data;
                return;
            }
            console.log(value.data[column_headers.indexOf('query')]);
            const query = value.data[column_headers.indexOf('query')];
            if (query) {
                script_queries.push(query);
            }
        });
        setScriptQueries(script_queries);

    }

    const handleOnError = (err, file, inputElem, reason) => {
        console.log(err)
    }

    const runScript = () => {
        if (getScriptIndex < getScriptQueries.length) {
            setInputState(getScriptQueries[getScriptIndex]);
            setScriptIndex(getScriptIndex + 1);

            submit(getScriptQueries[getScriptIndex]);
        }
    }

    const handleOnRemoveFile = (data) => {
        console.log('---------------------------')
        console.log(data)
        console.log('---------------------------')
        setScriptQueries([]);
    }

    let leftOpen = getState.leftOpen ? 'open' : 'closed';
    let rightOpen = getState.rightOpen ? 'open' : 'closed';
    let label = 'test'


    const columns = [
        {field: 'id', headerName: 'ID', flex: 1},
        {
            field: 'firstName',
            headerName: 'First name',
            flex: 1,
            editable: true,
        },
        {
            field: 'lastName',
            headerName: 'Last name',
            flex: 1,
            editable: true,
        },
        {
            field: 'age',
            headerName: 'Age',
            type: 'number',
            flex: 1,
            editable: true,
        },
        {
            field: 'fullName',
            headerName: 'Full name',
            description: 'This column has a value getter and is not sortable.',
            sortable: false,
            flex: 1,
            valueGetter: (params) =>
                `${params.getValue(params.id, 'firstName') || ''} ${
                    params.getValue(params.id, 'lastName') || ''
                }`,
        },
    ];

    const rows = [
        {id: 1, lastName: 'Snow', firstName: 'Jon', age: 35},
        {id: 2, lastName: 'Lannister', firstName: 'Cersei', age: 42},
        {id: 3, lastName: 'Lannister', firstName: 'Jaime', age: 45},
        {id: 4, lastName: 'Stark', firstName: 'Arya', age: 16},
        {id: 5, lastName: 'Targaryen', firstName: 'Daenerys', age: null},
        {id: 6, lastName: 'Melisandre', firstName: null, age: 150},
        {id: 7, lastName: 'Clifford', firstName: 'Ferrara', age: 44},
        {id: 8, lastName: 'Frances', firstName: 'Rossini', age: 36},
        {id: 9, lastName: 'Roxie', firstName: 'Harvey', age: 65},
    ];

    return (
        <div className="App">
            <div id='layout'>

                {/*<div id='left' className={leftOpen}>*/}
                {/*    <div className='icon'*/}
                {/*         onClick={toggleSidebar}>*/}
                {/*        &equiv;*/}
                {/*    </div>*/}
                {/*    <div className='bordered'>*/}
                {/*        <div className={`sidebar ${leftOpen}`}>*/}
                {/*            <div className='header'>*/}
                {/*                <h2 className='titleLeft'>*/}
                {/*                    Data Panel*/}
                {/*                </h2>*/}
                {/*            </div>*/}
                {/*            <div className='leftContent'>*/}
                {/*                <div className='header'>*/}
                {/*                    <div className='item'>*/}
                {/*                        <h3>Source Table</h3>*/}
                {/*                    </div>*/}
                {/*                </div>*/}
                {/*                <div className='header'>*/}
                {/*                    <div className='item'>*/}
                {/*                        <h3>Data Tables</h3>*/}
                {/*                    </div>*/}
                {/*                </div>*/}
                {/*                {*/}
                {/*                    getDataTables.status === 200 ?*/}
                {/*                        getDataTables.data.map((item, key) => {*/}
                {/*                            return <div className='header'>*/}
                {/*                                <p className='listItem'>{item}</p>*/}
                {/*                            </div>*/}
                {/*                        }) : ''*/}
                {/*                }*/}
                {/*                <div className='header'>*/}
                {/*                    <div className='item'>*/}
                {/*                        <h3>Columns</h3>*/}
                {/*                    </div>*/}
                {/*                </div>*/}
                {/*                {*/}
                {/*                    getColumns.status === 200 ?*/}
                {/*                        getColumns.data.map((item, key) => {*/}
                {/*                            return <div className='header'>*/}
                {/*                                <p className='listItem'>{item}</p>*/}
                {/*                            </div>*/}
                {/*                        }) : ''*/}
                {/*                }*/}
                {/*            </div>*/}
                {/*        </div>*/}
                {/*    </div>*/}
                {/*</div>*/}

                <div id='main'>
                    <header>
                        {versionNumber}
                    </header>
                    <div className='header'>
                        <div className={`
                      title
                      ${'left-' + leftOpen}
                      ${'right-' + rightOpen}
                  `}>
                            <Container>
                                <div className='searchBar'>
                                    <Row>
                                        <Col sm={1}>
                                            <div className='mic'>
                                                <ReactVoiceInput
                                                    onSpeechStart={startListening}
                                                    onResult={onResult}
                                                    onEnd={handleSubmit2}
                                                    onClick={startListening}

                                                >
                                                </ReactVoiceInput>
                                            </div>
                                        </Col>
                                        <Col sm={1}>
                                            <form className='input' onSubmit={handleSubmit}>

                                                <div className='input'>
                                                    <input className='input' type='text'
                                                           placeholder='Speak or Start Typing..'
                                                           value={getInputState}
                                                           onChange={handleChange}/>
                                                </div>
                                            </form>
                                        </Col>
                                    </Row>
                                    <Row>
                                        <Col>
                                            <Container className='Script'><Checkbox label={'Use Script'}
                                                                                    defaultUnchecked
                                                                                    onChange={handleScriptBoxChange}/> Script
                                                {getIsScript ?
                                                    <CSVReader
                                                        onDrop={handleOnDrop}
                                                        onError={handleOnError}
                                                        addRemoveButton
                                                        removeButtonColor='#659cef'
                                                        onRemoveFile={handleOnRemoveFile}
                                                    >
                                                        <span>Drop CSV file here or click to upload.</span>
                                                    </CSVReader> : ''}
                                                {getIsScript ?
                                                    getScriptQueries.map((d) => <li>{d}</li>) : ''}
                                                {getIsScript ? <Button variant="contained"
                                                                       onClick={runScript}>Next</Button> : ''}
                                            </Container>
                                        </Col>
                                    </Row>
                                </div>
                            </Container>
                        </div>
                    </div>
                    <header className="App-header">
                        <div className='loading'>{submitting ?
                            <div>
                                {/*<h3>LOADING</h3>*/}
                                <img src={spinner} alt="logo"/>
                            </div>
                            : getListening ?
                                'Listening..'
                                :
                                ''}</div>
                        {getListening ?
                            ''
                            :
                            <div style={{
                                height: '100%',
                                width: '80%',
                                'padding-bottom': '5%',
                                'padding-top': '5%'
                            }}>{!submitting ?
                                getTable.status === 200 && !getError ?
                                    <div>

                                        {getTable.data.distinct_values && getTable.data.distinct_values_table.rows.length ?
                                            <div style={{
                                                height: '100%',
                                                width: '80%',
                                                'padding-bottom': '5%',
                                                'padding-top': '5%'
                                            }}>
                                                <div style={{'text-align': 'left', 'margin-left': '2vw'}}>
                                                    Choose One or more
                                                </div>
                                                <div style={{
                                                    height: '100%',
                                                    width: '80% !important',
                                                    'padding-bottom': '5%',
                                                    'padding-top': '5%'
                                                }}>
                                                    <DataGrid
                                                        rows={getTable.data.distinct_values_table.rows}
                                                        columns={getTable.data.distinct_values_table.cols}
                                                        pageSize={5}
                                                        checkboxSelection
                                                        onSelectionModelChange={(newSelection) => {
                                                            setFilters(newSelection);
                                                        }}
                                                    />
                                                </div>
                                            </div>
                                            :
                                            <div>
                                                {getTable.data.tables.map((value, index) =>
                                                    <div style={{
                                                        width: '100%',
                                                        'padding-bottom': '2%',
                                                        'padding-top': '2%'
                                                    }}>
                                                        {getTable.data.follow_up ?
                                                            <div>
                                                                <h4>{getPrevQuery}</h4>
                                                                <h4>{getTable.data.query}</h4>
                                                            </div>
                                                            :
                                                            <h4>{getTable.data.query}</h4>
                                                        }
                                                        <h2
                                                            style={{
                                                                'margin-bottom': '150px'
                                                            }}> {value.total}
                                                        </h2>
                                                        {value.total > 0 ?
                                                            <div>
                                                                <div className='Chart'>
                                                                    <Chart
                                                                        width={'100%'}
                                                                        height={'800px'}
                                                                        chartType={getChartType}
                                                                        loader={<div>Loading Chart</div>}
                                                                        data={value.chart_data}
                                                                        options={{
                                                                            isStacked: true,
                                                                            fontSize: 36,
                                                                            titleTextStyle: {
                                                                                fontSize: 32,
                                                                            },
                                                                            annotations: {
                                                                                textStyle: {
                                                                                    fontSize: 32,
                                                                                }
                                                                            },
                                                                            legend: {
                                                                                position: "right",
                                                                                alignment: "start",
                                                                                maxLines: 10,
                                                                                textStyle: {fontSize: 24}
                                                                            },
                                                                            hAxis: {
                                                                                titleTextStyle: {
                                                                                    fontSize: 32,
                                                                                    bold: true,
                                                                                    italic: false
                                                                                }
                                                                            },
                                                                            vAxis: {
                                                                                titleTextStyle: {
                                                                                    fontSize: 32,
                                                                                    bold: true,
                                                                                    italic: false
                                                                                }
                                                                            },
                                                                        }}
                                                                    />
                                                                    <Button
                                                                        onClick={handleOpen}>Chart Properties
                                                                    </Button>
                                                                    <Button
                                                                        onClick={handleChartSwitch}>Switch chart
                                                                    </Button>
                                                                </div>
                                                                <div>
                                                                    <DataGrid
                                                                        // theme={theme}
                                                                        rows={value.rows}
                                                                        columns={value.cols}
                                                                        pageSize={100}
                                                                        checkboxSelection
                                                                    />
                                                                </div>
                                                            </div>
                                                            : ''
                                                        }
                                                        {/*<Button*/}
                                                        {/*    onClick={handleOpen}>Table Properties*/}
                                                        {/*</Button>*/}
                                                    </div>)}
                                                {getTable.data.totals ?
                                                    <div style={{
                                                        width: '100%',
                                                        'padding-bottom': '5%',
                                                        'padding-top': '5%'
                                                    }}>
                                                        <div style={{
                                                            'margin-top': '15vw',
                                                            'text-align': 'left',
                                                            'margin-left': '2vw'
                                                        }}>
                                                            Quality Table
                                                        </div>
                                                        <div style={{
                                                            width: '100%',
                                                            'padding-bottom': '5%',
                                                            'padding-top': '5%'
                                                        }}>
                                                            <DataGrid
                                                                // theme={theme}
                                                                rows={getTable.data.totals_table.rows}
                                                                columns={getTable.data.totals_table.cols}
                                                                pageSize={5}
                                                                checkboxSelection
                                                            />
                                                        </div>
                                                    </div> : ''}
                                            </div>}

                                        {getTable.data.distinct_values && getTable.data.distinct_values_table.rows.length ?
                                            <div style={{
                                                height: '100%',
                                                width: '100%',
                                                'padding-bottom': '5%',
                                                'padding-top': '5%',
                                                'font-size': 'xx-large'
                                            }}>
                                                <Button
                                                    onClick={handleFilter}>Go
                                                </Button>
                                                <Button
                                                    onClick={handleOpen}>Table Properties
                                                </Button>
                                            </div>
                                            :
                                            ''}
                                        {/*<Modal*/}
                                        {/*    open={open}*/}
                                        {/*    onClose={handleClose}*/}
                                        {/*    aria-labelledby="modal-modal-title"*/}
                                        {/*    aria-describedby="modal-modal-description"*/}
                                        {/*>*/}
                                        {/*    <Box sx={style}>*/}
                                        {/*        <Typography id="modal-modal-title" variant="h5" component="h2"*/}
                                        {/*                    textAlign={'center'}>*/}
                                        {/*            Properties*/}
                                        {/*        </Typography>*/}
                                        {/*        <div>*/}
                                        {/*            <div className='PropertiesField'>*/}
                                        {/*                <TextField*/}
                                        {/*                    label="Chart Name"*/}
                                        {/*                    defaultValue=""*/}
                                        {/*                    variant="standard"*/}
                                        {/*                    contentEditable="false"*/}
                                        {/*                />*/}
                                        {/*            </div>*/}
                                        {/*            <div className='PropertiesLabel'>*/}
                                        {/*                <label>Command</label>*/}
                                        {/*            </div>*/}
                                        {/*            <div className='PropertiesField'>*/}
                                        {/*                <label>{getTable.data.query}</label>*/}
                                        {/*            </div>*/}
                                        {/*            <div className='PropertiesLabel'>*/}
                                        {/*                <label>SQL</label>*/}
                                        {/*            </div>*/}
                                        {/*            <div className='PropertiesField'>*/}
                                        {/*                <ReactCodeSnippet lang="sql"*/}
                                        {/*                                  code={getTable.data.queries[0].toUpperCase()}/>*/}
                                        {/*            </div>*/}
                                        {/*            <div className='PropertiesField'>*/}
                                        {/*                <TextField*/}
                                        {/*                    label="Rows"*/}
                                        {/*                    defaultValue={getTable.data.rows}*/}
                                        {/*                    variant="standard"*/}
                                        {/*                />*/}
                                        {/*            </div>*/}
                                        {/*            <div className='PropertiesField'>*/}
                                        {/*                <TextField*/}
                                        {/*                    label="Columns"*/}
                                        {/*                    defaultValue={getTable.data.tables[0].chart_data[0][0]}*/}
                                        {/*                    variant="standard"*/}
                                        {/*                />*/}
                                        {/*            </div>*/}
                                        {/*            <div className='PropertiesField'>*/}
                                        {/*                <TextField*/}
                                        {/*                    label="Data"*/}
                                        {/*                    defaultValue="Count"*/}
                                        {/*                    variant="standard"*/}
                                        {/*                />*/}
                                        {/*            </div>*/}
                                        {/*            <div className='PropertiesField'>*/}
                                        {/*                <TextField*/}
                                        {/*                    label="Active Filters"*/}
                                        {/*                    defaultValue={getTable.data.conditions}*/}
                                        {/*                    variant="standard"*/}
                                        {/*                />*/}
                                        {/*            </div>*/}
                                        {/*        </div>*/}
                                        {/*        <Typography id="modal-modal-title" variant="h6" component="h3"*/}
                                        {/*                    textAlign={'center'}>*/}
                                        {/*            Settings*/}
                                        {/*        </Typography>*/}
                                        {/*        <div>*/}
                                        {/*            <div className='PropertiesField'>*/}
                                        {/*                <TextField*/}
                                        {/*                    label="Chart Title"*/}
                                        {/*                    variant="standard"*/}
                                        {/*                />*/}
                                        {/*            </div>*/}
                                        {/*            <div className='PropertiesField'>*/}
                                        {/*                <TextField*/}
                                        {/*                    label="X-Axis Label"*/}
                                        {/*                    defaultValue={getTable.data.tables[0].chart_data[0][0]}*/}
                                        {/*                    variant="standard"*/}
                                        {/*                />*/}
                                        {/*            </div>*/}
                                        {/*            <div className='PropertiesField'>*/}
                                        {/*                <TextField*/}
                                        {/*                    label="Y-Axis Label"*/}
                                        {/*                    defaultValue="Calls Count"*/}
                                        {/*                    variant="standard"*/}
                                        {/*                />*/}
                                        {/*            </div>*/}
                                        {/*            <div>*/}
                                        {/*                <FormControl>*/}
                                        {/*                    <InputLabel>Weight</InputLabel>*/}
                                        {/*                    <Select*/}
                                        {/*                        label="Weight"*/}
                                        {/*                        value={weight}*/}
                                        {/*                        onChange={handleWeightChange}*/}
                                        {/*                        autoWidth*/}
                                        {/*                    >*/}
                                        {/*                        <MenuItem value="Thin">Thin</MenuItem>*/}
                                        {/*                        <MenuItem value="Medium">Medium</MenuItem>*/}
                                        {/*                        <MenuItem value="Thick">Thick</MenuItem>*/}
                                        {/*                    </Select>*/}
                                        {/*                </FormControl>*/}
                                        {/*            </div>*/}
                                        {/*            <div className='PropertiesCheckBox'>*/}
                                        {/*                Show Values in Labels*/}
                                        {/*                <Checkbox label={label} defaultUnchecked/>*/}
                                        {/*            </div>*/}
                                        {/*            <div className='PropertiesCheckBox'>*/}
                                        {/*                Show Percents in Labels*/}
                                        {/*                <Checkbox label={label} defaultUnchecked/>*/}
                                        {/*            </div>*/}
                                        {/*            <div className='PropertiesCheckBox'>*/}
                                        {/*                Exclude Final Period*/}
                                        {/*                <Checkbox label={label} defaultUnchecked/>*/}
                                        {/*            </div>*/}
                                        {/*            <div className='PropertiesCheckBox'>*/}
                                        {/*                Exclude Totals*/}
                                        {/*                <Checkbox label={label} defaultUnchecked/>*/}
                                        {/*            </div>*/}
                                        {/*        </div>*/}
                                        {/*        <React.Fragment>*/}
                                        {/*            <Typography id="modal-modal-title" variant="h6" component="h3"*/}
                                        {/*                        textAlign={'center'}>*/}
                                        {/*                <Button variant="outlined"*/}
                                        {/*                        onClick={handleChildModalOpen}>Actions</Button>*/}
                                        {/*            </Typography>*/}
                                        {/*            <Modal*/}
                                        {/*                hideBackdrop*/}
                                        {/*                open={childModalOpen}*/}
                                        {/*                onClose={handleChildModalClose}*/}
                                        {/*                aria-labelledby="child-modal-title"*/}
                                        {/*                aria-describedby="child-modal-description"*/}
                                        {/*            >*/}
                                        {/*                <Box sx={{...style, width: 200}}>*/}
                                        {/*                    <div>*/}
                                        {/*                        <div>*/}
                                        {/*                            <Button variant="contained">Add/Remove Bars</Button>*/}
                                        {/*                        </div>*/}
                                        {/*                        <div>*/}
                                        {/*                            <Button variant="contained">Highlight*/}
                                        {/*                                Element</Button>*/}
                                        {/*                        </div>*/}
                                        {/*                        <div>*/}
                                        {/*                            <Button variant="contained">Format</Button>*/}
                                        {/*                        </div>*/}
                                        {/*                        <div>*/}
                                        {/*                            <Button variant="contained">Color</Button>*/}
                                        {/*                        </div>*/}
                                        {/*                        <div>*/}
                                        {/*                            <Button variant="contained">Reference Line</Button>*/}
                                        {/*                        </div>*/}
                                        {/*                        <div>*/}
                                        {/*                            <Button variant="contained">Reference Band</Button>*/}
                                        {/*                        </div>*/}
                                        {/*                        <div>*/}
                                        {/*                            <Button variant="contained">Filters</Button>*/}
                                        {/*                        </div>*/}
                                        {/*                        <div>*/}
                                        {/*                            <Button variant="contained">SPRT</Button>*/}
                                        {/*                        </div>*/}
                                        {/*                        <div>*/}
                                        {/*                            <Button variant="contained">Save</Button>*/}
                                        {/*                        </div>*/}
                                        {/*                        <div>*/}
                                        {/*                            <Button variant="contained">Share</Button>*/}
                                        {/*                        </div>*/}
                                        {/*                        <div>*/}
                                        {/*                            <Button variant="contained">Transpose</Button>*/}
                                        {/*                        </div>*/}
                                        {/*                        <div>*/}
                                        {/*                            <Button variant="contained">Embed Image</Button>*/}
                                        {/*                        </div>*/}
                                        {/*                        <Button onClick={handleChildModalClose}*/}
                                        {/*                                variant="contained">OK</Button>*/}
                                        {/*                        <Button onClick={handleChildModalClose}*/}
                                        {/*                                variant="contained">Quit</Button>*/}
                                        {/*                    </div>*/}
                                        {/*                </Box>*/}
                                        {/*            </Modal>*/}
                                        {/*        </React.Fragment>*/}
                                        {/*    </Box>*/}
                                        {/*</Modal>*/}
                                    </div>
                                    :
                                    getError ?
                                        'Unable to process query. Please try again.'
                                        :
                                        ''
                                :
                                ''}</div>}
                    </header>
                </div>

                {/*<div id='right' className={rightOpen}>*/}
                {/*    <div className='icon'*/}
                {/*         onClick={toggleSidebar}>*/}
                {/*        &equiv;*/}
                {/*    </div>*/}
                {/*    <div className='bordered'>*/}
                {/*        <div className={`sidebar ${rightOpen}`}>*/}
                {/*            <div className='header'>*/}
                {/*                <h2 className='titleRight'>*/}
                {/*                    Commands*/}
                {/*                </h2>*/}
                {/*            </div>*/}
                {/*            <div className='leftContent'>*/}
                {/*                <div className='header'>*/}
                {/*                    <h3 className='item'>*/}
                {/*                        Analytical Commands*/}
                {/*                    </h3>*/}
                {/*                </div>*/}
                {/*                <div className='header'>*/}
                {/*                    <p className='item'>*/}
                {/*                        Count*/}
                {/*                    </p>*/}
                {/*                </div>*/}
                {/*                <div className='header'>*/}
                {/*                    <p className='item'>*/}
                {/*                        Average*/}
                {/*                    </p>*/}
                {/*                </div>*/}
                {/*                <div className='header'>*/}
                {/*                    <p className='item'>*/}
                {/*                        Cross*/}
                {/*                    </p>*/}
                {/*                </div>*/}
                {/*                <div className='header'>*/}
                {/*                    <p className='item'>*/}
                {/*                        Compare*/}
                {/*                    </p>*/}
                {/*                </div>*/}
                {/*                <div className='header'>*/}
                {/*                    <h3 className='item'>*/}
                {/*                        Selection Commands*/}
                {/*                    </h3>*/}
                {/*                </div>*/}
                {/*                <div className='header'>*/}
                {/*                    <p className='item'>*/}
                {/*                        Breakout By*/}
                {/*                    </p>*/}
                {/*                </div>*/}
                {/*                <div className='header'>*/}
                {/*                    <p className='item'>*/}
                {/*                        Where / Filter*/}
                {/*                    </p>*/}
                {/*                </div>*/}
                {/*            </div>*/}
                {/*        </div>*/}
                {/*    </div>*/}
                {/*</div>*/}

            </div>
        </div>
    );
}

export default App;