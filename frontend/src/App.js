import React, {useEffect, useState, useReducer} from 'react';
import Popup from 'reactjs-popup';
import 'reactjs-popup/dist/index.css';
import spinner from './spinner.svg';
import './App.css';
import 'react-minimal-side-navigation/lib/ReactMinimalSideNavigation.css';
import axios from 'axios'
import {JsonToTable} from "react-json-to-table";
import {Container, Row, Col} from 'react-grid-system';
import Checkbox from '@mui/material/Checkbox';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Typography from '@mui/material/Typography';
import Modal from '@mui/material/Modal';
import ReactVoiceInput from 'react-voice-input'

const formReducer = (state, event) => {
    return {
        ...state,
        [event.name]: event.value
    }
}


// Make sure to bind modal to your appElement (https://reactcommunity.org/react-modal/accessibility/)
console.log(document.getElementById('main'))

function App() {

    const [getMessage, setGetMessage] = useState({})
    const [getTable, setGetTable] = useState({})
    const [formData, setFormData] = useReducer(formReducer, {});
    const [submitting, setSubmitting] = useState(false);
    const [getError, setError] = useState(false);
    const [getListening, setListening] = useState(false);
    const [getColumns, setColumns] = useState([]);
    const [getDataTables, setDataTables] = useState([]);
    const [getState, setState] = useState({
        leftOpen: true,
        rightOpen: true,
    });

    const [getPrevQuery, setPrevQuery] = useState('')
    const versionNumber = '2.0.7'

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


    let rows = []
    let cols = []

    const handleSubmit = event => {
        event.preventDefault();
        setSubmitting(true);
        axios.post('http://localhost:1234/answerz', {
            'text': getInputState.replace('  ', ' '),
            'prev_query': (getInputState.toLowerCase().includes('group by') || getInputState.toLowerCase().includes('down by') || getInputState.toLowerCase().includes('out by')) ? getPrevQuery : ''
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

    const handleChange = event => {
        // setFormData({
        //     name: event.target.name,
        //     value: event.target.value,
        // });
        setInputState(event.target.value.replace('  ', ' '));
    }

    const style = {
        position: 'absolute',
        top: '50%',
        left: '50%',
        transform: 'translate(-50%, -50%)',
        width: 400,
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
        axios.post('http://localhost:1234/answerz', {'text': getInputState})
            .then(response => {
                console.log("Responded", response)
                setGetTable(response)
                setSubmitting(false);
                setError(false);
            }).catch((error) => {
            setSubmitting(false);
            setError(true);
        });
    }

    const [open, setOpen] = React.useState(false);
    const handleOpen = () => setOpen(true);
    const handleClose = () => setOpen(false);
    const handleBoxChange = () => setGetTable(getTable);

    const toggleSidebar = (event) => {
        let key = `${event.currentTarget.parentNode.id}Open`;
        let otherKey = (key === 'leftOpen') ? 'rightOpen' : 'leftOpen';
        setState({[key]: !getState[key], [otherKey]: getState[otherKey]});
        console.log(getState);
    }

    const onEnd = () => {
        console.log('on end')
    }

    let leftOpen = getState.leftOpen ? 'open' : 'closed';
    let rightOpen = getState.rightOpen ? 'open' : 'closed';
    let languages = ['test', 'test2']
    let label = 'test'

    return (
        <div className="App">
            <div id='layout'>

                <div id='left' className={leftOpen}>
                    <div className='icon'
                         onClick={toggleSidebar}>
                        &equiv;
                    </div>
                    <div className='bordered'>
                        <div className={`sidebar ${leftOpen}`}>
                            <div className='header'>
                                <h2 className='titleLeft'>
                                    Data Panel
                                </h2>
                            </div>
                            <div className='leftContent'>
                                <div className='header'>
                                    <div className='item'>
                                        <h3>Source Table</h3>
                                    </div>
                                </div>
                                <div className='header'>
                                    <div className='item'>
                                        <h3>Data Tables</h3>
                                    </div>
                                </div>
                                {
                                    getDataTables.status === 200 ?
                                        getDataTables.data.map((item, key) => {
                                            return <div className='header'>
                                                <p className='listItem'>{item}</p>
                                            </div>
                                        }) : ''
                                }
                                <div className='header'>
                                    <div className='item'>
                                        <h3>Columns</h3>
                                    </div>
                                </div>
                                {
                                    getColumns.status === 200 ?
                                        getColumns.data.map((item, key) => {
                                            return <div className='header'>
                                                <p className='listItem'>{item}</p>
                                            </div>
                                        }) : ''
                                }
                            </div>
                        </div>
                    </div>
                </div>

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
                                                           placeholder='Speak or Start Typing..' value={getInputState}
                                                           onChange={handleChange}/>
                                                </div>
                                            </form>
                                        </Col>
                                        {/*<form className='input' onSubmit={handleSubmit}>*/}
                                        {/*    <input autoComplete="off" name="name"*/}
                                        {/*           placeholder='Speak or Type Command Here'*/}
                                        {/*           onChange={handleChange}/>*/}
                                        {/*</form>*/}
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
                            <div className='table'>{!submitting ?
                                getTable.status === 200 && !getError ?
                                    <div>
                                        {getTable.data.message.map((value, index) =>
                                            <div className='table-class'> {getTable.data.queries[index]} <JsonToTable
                                                json={value}/><Button
                                                onClick={handleOpen}>Table Properties</Button></div>)}
                                        {getTable.data.other_result ?
                                            <div className='table-class'><JsonToTable
                                                json={getTable.data.other_result}/></div>
                                            :
                                            ''}
                                        <Modal
                                            open={open}
                                            onClose={handleClose}
                                            aria-labelledby="modal-modal-title"
                                            aria-describedby="modal-modal-description"
                                        >
                                            <Box sx={style}>
                                                <Typography id="modal-modal-title" variant="h6" component="h2">
                                                    Table Properties
                                                </Typography>
                                                <h4>Table Name: {formData.name}</h4>
                                                <h4>Rows: {formData.name}</h4>
                                                <h4>Columns: {formData.name}</h4>
                                                <div>
                                                    <Checkbox label={label} defaultUnchecked
                                                              onChange={handleBoxChange}/> Show Column Totals at Bottom
                                                </div>
                                                <div>
                                                    <Checkbox label={label} defaultUnchecked/> Show Column Totals at Top
                                                </div>
                                                <div>
                                                    <Checkbox label={label} defaultUnchecked/> Show Row Totals on Left
                                                </div>
                                                <div>
                                                    <Checkbox label={label} defaultUnchecked/> Show Row Totals at Right
                                                </div>
                                                <div>
                                                    <Checkbox label={label} defaultUnchecked/> Show Percent of Row
                                                    Totals
                                                </div>
                                                <div>
                                                    <Checkbox label={label} defaultUnchecked/> Show Percent of Column
                                                    Totals
                                                </div>
                                                <div>
                                                    <Checkbox label={label} defaultUnchecked/> Show Percent of All
                                                </div>
                                                <div>
                                                    <Checkbox label={label} defaultUnchecked/> Show Counts
                                                </div>
                                            </Box>
                                        </Modal>
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

                <div id='right' className={rightOpen}>
                    <div className='icon'
                         onClick={toggleSidebar}>
                        &equiv;
                    </div>
                    <div className='bordered'>
                        <div className={`sidebar ${rightOpen}`}>
                            <div className='header'>
                                <h2 className='titleRight'>
                                    Commands
                                </h2>
                            </div>
                            <div className='leftContent'>
                                <div className='header'>
                                    <h3 className='item'>
                                        Analytical Commands
                                    </h3>
                                </div>
                                <div className='header'>
                                    <p className='item'>
                                        Count
                                    </p>
                                </div>
                                <div className='header'>
                                    <p className='item'>
                                        Average
                                    </p>
                                </div>
                                <div className='header'>
                                    <p className='item'>
                                        Cross
                                    </p>
                                </div>
                                <div className='header'>
                                    <p className='item'>
                                        Compare
                                    </p>
                                </div>
                                <div className='header'>
                                    <h3 className='item'>
                                        Selection Commands
                                    </h3>
                                </div>
                                <div className='header'>
                                    <p className='item'>
                                        Breakout By
                                    </p>
                                </div>
                                <div className='header'>
                                    <p className='item'>
                                        Where / Filter
                                    </p>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}

export default App;